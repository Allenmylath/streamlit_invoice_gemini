import os
import uuid
import asyncio
import datetime
import logging
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
logger = logging.getLogger("InvoiceBatchProcessor")

class InvoiceBatchProcessor:
    def __init__(
        self,
        s3_bucket: str = "invoices-data",
        requests_per_minute: int = 10
    ):
        """
        Initialize the batch invoice processor with API keys from environment and rate limiter
        
        Args:
            s3_bucket: S3 bucket name to store processed results
            requests_per_minute: Maximum Gemini API requests per minute (default: 10)
        """
        # Get API keys from environment variables
        self.gemini_api_key = os.environ.get("GEMINI_API_KEY")
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        
        # Validate that required API keys are present
        if not self.gemini_api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required")
        self.s3_bucket = s3_bucket
        
        # Create a rate limiter (10 requests per minute = 1 request per 6 seconds)
        self.rate_limiter = AsyncLimiter(requests_per_minute, 60)
        
        # Configure Gemini API
        genai.configure(api_key=self.gemini_api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        
    async def process_batch(
        self, 
        invoice_files: List[Tuple[bytes, str, str]]
    ) -> Dict[str, Any]:
        """
        Process a batch of invoice files concurrently with rate limiting
        
        Args:
            invoice_files: List of tuples (file_bytes, file_type, file_name)
            
        Returns:
            Dict with processing statistics and batch information
        """
        # Create a batch ID and timestamp
        batch_id = str(uuid.uuid4())
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_folder = f"invoice_processing/{batch_id}_{timestamp}/"
        
        logger.info(f"Starting batch processing of {len(invoice_files)} invoices")
        logger.info(f"Batch ID: {batch_id}")
        logger.info(f"S3 folder: {s3_folder}")
        
        # Process files concurrently with rate limiting
        tasks = []
        for file_bytes, file_type, file_name in invoice_files:
            # Skip non-image files
            if not file_type.startswith('image/'):
                logger.warning(f"Skipping non-image file: {file_name}")
                continue
                
            # Create task for processing and uploading
            task = asyncio.create_task(
                self._process_and_upload(file_bytes, file_type, file_name, batch_id, s3_folder)
            )
            tasks.append(task)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Compile statistics
        successful = sum(1 for r in results if r.get("success", False))
        failed = len(results) - successful
        
        # Create a summary file
        summary = {
            "batch_id": batch_id,
            "timestamp": timestamp,
            "total_files": len(invoice_files),
            "total_processed": len(results),
            "successful": successful,
            "failed": failed,
            "s3_folder": s3_folder,
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
                    Key=f"{s3_folder}_summary.json",
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
        file_name: str,
        batch_id: str,
        s3_folder: str
    ) -> Dict[str, Any]:
        """
        Helper method to process a single invoice and upload results to S3
        
        Args:
            file_bytes: Raw bytes of the image file
            file_type: MIME type of the file
            file_name: Original filename
            batch_id: UUID for the current batch
            s3_folder: S3 folder path for this batch
            
        Returns:
            Dict with processing results and S3 metadata
        """
        # Process the invoice with rate limiting
        result = await self._process_single_invoice(file_bytes, file_type, file_name)
        
        # Upload to S3
        result = await self._upload_to_s3(result, batch_id, s3_folder)
        
        return result
    
    async def _process_single_invoice(
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
                Extract info from the invoice:
                
                Make sure to return only valid markdown.
                """
                
                # Generate content - need to run in thread pool since genai is synchronous
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
    
    async def _upload_to_s3(
        self, 
        result: Dict[str, Any],
        batch_id: str,
        s3_folder: str
    ) -> Dict[str, Any]:
        """
        Upload processing result to S3
        
        Args:
            result: Dictionary containing processing results
            batch_id: UUID for the current batch
            s3_folder: S3 folder path for this batch
            
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
            
            async with session.client("s3") as s3:
                if result["success"]:
                    # Upload the markdown content
                    s3_key = f"{s3_folder}{file_base}.md"
                    await s3.put_object(
                        Bucket=self.s3_bucket,
                        Key=s3_key,
                        Body=result["markdown"].encode('utf-8'),
                        ContentType="text/markdown",
                        Metadata={
                            "original_filename": result["file_name"],
                            "processing_timestamp": result["timestamp"],
                            "batch_id": batch_id
                        }
                    )
                else:
                    # Upload error information
                    s3_key = f"{s3_folder}errors/{file_base}.error.txt"
                    await s3.put_object(
                        Bucket=self.s3_bucket,
                        Key=s3_key,
                        Body=result["error"].encode('utf-8'),
                        ContentType="text/plain",
                        Metadata={
                            "original_filename": result["file_name"],
                            "processing_timestamp": result["timestamp"],
                            "batch_id": batch_id
                        }
                    )
                
            result["s3_key"] = s3_key
            result["s3_bucket"] = self.s3_bucket
            return result
            
        except Exception as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            result["s3_error"] = str(e)
            return result
"""
# Usage example
async def process_invoices(
    invoice_files: List[Tuple[bytes, str, str]],
    s3_bucket: str = "invoices-data"
) -> Dict[str, Any]:
    """
    Process a batch of invoice files using environment variable credentials
    
    Args:
        invoice_files: List of tuples (file_bytes, file_type, file_name)
        s3_bucket: S3 bucket name
        
    Returns:
        Dict with processing summary
    """
    processor = InvoiceBatchProcessor(
        s3_bucket=s3_bucket,
        requests_per_minute=10
    )
    
    return await processor.process_batch(invoice_files)
"""
