import os
import uuid
import asyncio
import aiohttp
import datetime
import logging
import boto3
from io import BytesIO
from PIL import Image
import google.generativeai as genai
import aioboto3
from typing import List, Dict, Any, Optional, Tuple
from botocore.exceptions import ClientError
from aiolimiter import AsyncLimiter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("InvoiceProcessor")

class InvoiceProcessor:
    def __init__(
        self, 
        gemini_api_key: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        s3_bucket: str = "invoices-data",
        requests_per_minute: int = 10
    ):
        """
        Initialize the invoice processor with API keys and rate limiter
        
        Args:
            gemini_api_key: API key for Google's Gemini API
            aws_access_key_id: AWS access key (optional if using IAM roles)
            aws_secret_access_key: AWS secret key (optional if using IAM roles)
            s3_bucket: S3 bucket name to store processed results
            requests_per_minute: Maximum Gemini API requests per minute (default: 10)
        """
        self.gemini_api_key = gemini_api_key
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_bucket = s3_bucket
        
        # Create a rate limiter (10 requests per minute = 1 request per 6 seconds)
        self.rate_limiter = AsyncLimiter(requests_per_minute, 60)
        
        # Configure Gemini API
        genai.configure(api_key=gemini_api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Create a processing ID for this batch
        self.batch_id = str(uuid.uuid4())
        self.timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.s3_folder = f"invoice_processing/{self.batch_id}_{self.timestamp}/"
        
    async def process_single_invoice(
        self, 
        image_data: bytes, 
        file_type: str, 
        file_name: str
    ) -> Dict[str, Any]:
        """
        Process a single invoice with rate limiting
        
        Args:
            image_data: Raw bytes of the image file
            file_type: MIME type of the file
            file_name: Original filename
            
        Returns:
            Dict containing processing results and metadata
        """
        # Apply rate limiting
        async with self.rate_limiter:
            try:
                logger.info(f"Processing invoice: {file_name}")
                
                # Open image
                image = Image.open(BytesIO(image_data))
                
                # Define the prompt for invoice extraction
                prompt = """
                Extract detailed information from this invoice:
                
                1. Invoice number
                2. Date
                3. Vendor/Company name
                4. Total amount
                5. Line items with descriptions and prices
                6. Payment terms
                7. Contact information
                
                Format the output as clean markdown with appropriate headers and tables.
                """
                
                # Generate content
                response = await asyncio.to_thread(
                    self.model.generate_content,
                    [prompt, image]
                )
                
                # Extract the text from the response
                markdown_str = response.text.strip()
                
                return {
                    "success": True,
                    "file_name": file_name,
                    "markdown": markdown_str,
                    "timestamp": datetime.datetime.now().isoformat()
                }
                
            except Exception as e:
                logger.error(f"Error processing {file_name}: {str(e)}")
                return {
                    "success": False,
                    "file_name": file_name,
                    "error": str(e),
                    "timestamp": datetime.datetime.now().isoformat()
                }
    
    async def upload_to_s3(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upload processing result to S3
        
        Args:
            result: Dictionary containing processing results
            
        Returns:
            Dict with updated S3 metadata
        """
        try:
            # Create S3 session with aioboto3
            session = aioboto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key
            )
            
            # Generate a unique filename for the result
            file_base = os.path.splitext(result["file_name"])[0]
            s3_key = f"{self.s3_folder}{file_base}.md"
            
            async with session.client("s3") as s3:
                if result["success"]:
                    # Upload the markdown content
                    await s3.put_object(
                        Bucket=self.s3_bucket,
                        Key=s3_key,
                        Body=result["markdown"].encode('utf-8'),
                        ContentType="text/markdown",
                        Metadata={
                            "original_filename": result["file_name"],
                            "processing_timestamp": result["timestamp"],
                            "batch_id": self.batch_id
                        }
                    )
                else:
                    # Upload error information
                    await s3.put_object(
                        Bucket=self.s3_bucket,
                        Key=f"{self.s3_folder}errors/{result['file_name']}.error.txt",
                        Body=result["error"].encode('utf-8'),
                        ContentType="text/plain",
                        Metadata={
                            "original_filename": result["file_name"],
                            "processing_timestamp": result["timestamp"],
                            "batch_id": self.batch_id
                        }
                    )
                
            result["s3_key"] = s3_key
            result["s3_bucket"] = self.s3_bucket
            return result
            
        except Exception as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            result["s3_error"] = str(e)
            return result
    
    async def process_batch(
        self, 
        invoice_files: List[Tuple[bytes, str, str]]
    ) -> Dict[str, Any]:
        """
        Process a batch of invoice files concurrently with rate limiting
        
        Args:
            invoice_files: List of tuples (file_bytes, file_type, file_name)
            
        Returns:
            Dict with processing statistics
        """
        logger.info(f"Starting batch processing of {len(invoice_files)} invoices")
        logger.info(f"Batch ID: {self.batch_id}")
        logger.info(f"S3 folder: {self.s3_folder}")
        
        # Process files concurrently with rate limiting
        tasks = []
        for file_bytes, file_type, file_name in invoice_files:
            # Skip non-image files
            if not file_type.startswith('image/'):
                logger.warning(f"Skipping non-image file: {file_name}")
                continue
                
            # Create task for processing and uploading
            task = asyncio.create_task(
                self._process_and_upload(file_bytes, file_type, file_name)
            )
            tasks.append(task)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Compile statistics
        successful = sum(1 for r in results if r.get("success", False))
        failed = len(results) - successful
        
        # Create a summary file
        summary = {
            "batch_id": self.batch_id,
            "timestamp": self.timestamp,
            "total_files": len(invoice_files),
            "successful": successful,
            "failed": failed,
            "s3_folder": self.s3_folder,
            "files": [{
                "file_name": r["file_name"],
                "success": r.get("success", False),
                "s3_key": r.get("s3_key", None),
                "error": r.get("error", None) if not r.get("success", False) else None
            } for r in results]
        }
        
        # Upload summary to S3
        try:
            session = aioboto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key
            )
            
            async with session.client("s3") as s3:
                await s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=f"{self.s3_folder}_summary.json",
                    Body=str(summary).encode('utf-8'),
                    ContentType="application/json"
                )
        except Exception as e:
            logger.error(f"Error uploading summary to S3: {str(e)}")
            
        logger.info(f"Batch processing complete: {successful} successful, {failed} failed")
        return summary
    
    async def _process_and_upload(
        self, 
        file_bytes: bytes, 
        file_type: str, 
        file_name: str
    ) -> Dict[str, Any]:
        """
        Helper method to process a single invoice and upload results to S3
        
        Args:
            file_bytes: Raw bytes of the image file
            file_type: MIME type of the file
            file_name: Original filename
            
        Returns:
            Dict with processing results and S3 metadata
        """
        # Process the invoice
        result = await self.process_single_invoice(file_bytes, file_type, file_name)
        
        # Upload to S3
        result = await self.upload_to_s3(result)
        
        return result

# Example usage
async def main():
    # Configuration
    gemini_api_key = os.environ.get("GEMINI_API_KEY")
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    s3_bucket = os.environ.get("S3_BUCKET", "invoices-data")
    
    # Initialize processor
    processor = InvoiceProcessor(
        gemini_api_key=gemini_api_key,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        s3_bucket=s3_bucket,
        requests_per_minute=10
    )
    
    # Simulated batch of invoice files
    # In a real application, these would come from your file upload system
    invoice_files = []
    
    # Example reading files from a directory
    invoice_dir = "invoice_images"
    if os.path.exists(invoice_dir):
        for filename in os.listdir(invoice_dir):
            if filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                file_path = os.path.join(invoice_dir, filename)
                with open(file_path, 'rb') as f:
                    file_bytes = f.read()
                    
                # Determine file type
                if filename.lower().endswith('.png'):
                    file_type = 'image/png'
                else:
                    file_type = 'image/jpeg'
                    
                invoice_files.append((file_bytes, file_type, filename))
    
    # Process the batch
    if invoice_files:
        results = await processor.process_batch(invoice_files)
        print(f"Processing complete. Check S3 folder: {processor.s3_folder}")
    else:
        print("No invoice files found")

if __name__ == "__main__":
    asyncio.run(main())
