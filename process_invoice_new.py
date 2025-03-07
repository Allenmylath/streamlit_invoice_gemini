import os
import uuid
import asyncio
import datetime
import logging
import traceback
from io import BytesIO
from PIL import Image
import google.generativeai as genai
import aioboto3
from typing import List, Dict, Any, Optional, Tuple
from botocore.exceptions import ClientError
from aiolimiter import AsyncLimiter

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG level for more verbose output
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger("InvoiceBatchProcessor")

class InvoiceBatchProcessor:
    def __init__(
        self,
        s3_bucket: str = "invoices-data-dataastra",
        requests_per_minute: int = 10
    ):
        """
        Initialize the batch invoice processor with API keys from environment and rate limiter
        
        Args:
            s3_bucket: S3 bucket name to store processed results
            requests_per_minute: Maximum Gemini API requests per minute (default: 10)
        """
        logger.info(f"Initializing InvoiceBatchProcessor with bucket: {s3_bucket}, rate limit: {requests_per_minute} rpm")
        
        # Get API keys from environment variables
        self.gemini_api_key = os.environ.get("GEMINI_API_KEY")
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        
        # Validate that required API keys are present
        if not self.gemini_api_key:
            logger.error("GEMINI_API_KEY environment variable is missing")
            raise ValueError("GEMINI_API_KEY environment variable is required")
        
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            logger.warning("AWS credentials may be missing from environment variables")
        
        self.s3_bucket = s3_bucket
        
        # Create a rate limiter (10 requests per minute = 1 request per 6 seconds)
        self.rate_limiter = AsyncLimiter(requests_per_minute, 60)
        logger.debug(f"Rate limiter configured: {requests_per_minute} requests per minute")
        
        # Configure Gemini API
        logger.debug("Configuring Gemini API client")
        genai.configure(api_key=self.gemini_api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        logger.info("InvoiceBatchProcessor initialization complete")
        
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
        
        # Log file types for debugging
        file_types = [f_type for _, f_type, _ in invoice_files]
        logger.debug(f"File types in batch: {file_types}")
        
        # Process files concurrently with rate limiting
        tasks = []
        skipped_count = 0
        
        for idx, (file_bytes, file_type, file_name) in enumerate(invoice_files):
            logger.debug(f"Preparing file {idx+1}/{len(invoice_files)}: {file_name} ({file_type}, {len(file_bytes)} bytes)")
            
            # Skip non-image files
            if not file_type.startswith('image/'):
                logger.warning(f"Skipping non-image file: {file_name} with type {file_type}")
                skipped_count += 1
                continue
                
            # Create task for processing and uploading
            logger.debug(f"Creating processing task for: {file_name}")
            task = asyncio.create_task(
                self._process_and_upload(file_bytes, file_type, file_name, batch_id, s3_folder)
            )
            tasks.append(task)
        
        if skipped_count > 0:
            logger.warning(f"Skipped {skipped_count} non-image files")
        
        logger.info(f"Created {len(tasks)} processing tasks")
        
        # Wait for all tasks to complete
        logger.info("Waiting for all processing tasks to complete...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions that might have been returned
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task {i} failed with exception: {str(result)}")
                # Create a failure record
                processed_results.append({
                    "success": False,
                    "file_name": invoice_files[i][2] if i < len(invoice_files) else f"unknown_file_{i}",
                    "error": f"Unhandled exception: {str(result)}",
                    "timestamp": datetime.datetime.now().isoformat()
                })
            else:
                processed_results.append(result)
        
        # Compile statistics
        successful = sum(1 for r in processed_results if r.get("success", False))
        failed = len(processed_results) - successful
        
        logger.info(f"Processing complete: {successful} successful, {failed} failed")
        
        # Create a summary file
        logger.debug("Creating batch summary")
        summary = {
            "batch_id": batch_id,
            "timestamp": timestamp,
            "total_files": len(invoice_files),
            "total_processed": len(processed_results),
            "successful": successful,
            "failed": failed,
            "s3_folder": s3_folder,
            "files": [{
                "file_name": r["file_name"],
                "success": r.get("success", False),
                "s3_key": r.get("s3_key", None),
                "error": r.get("error", None) if not r.get("success", False) else None
            } for r in processed_results]
        }
        
        # Upload summary to S3
        logger.info("Uploading batch summary to S3")
        try:
            session = aioboto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key
            )
            
            logger.debug(f"Creating S3 client for summary upload")
            async with session.client("s3") as s3:
                summary_key = f"{s3_folder}_summary.json"
                logger.debug(f"Uploading summary to {self.s3_bucket}/{summary_key}")
                await s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=summary_key,
                    Body=str(summary).encode('utf-8'),
                    ContentType="application/json"
                )
                logger.info(f"Summary uploaded successfully to s3://{self.s3_bucket}/{summary_key}")
        except Exception as e:
            logger.error(f"Error uploading summary to S3: {str(e)}")
            logger.error(f"Exception traceback: {traceback.format_exc()}")
            
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
        logger.debug(f"Processing and uploading: {file_name}")
        
        try:
            # Process the invoice with rate limiting
            logger.debug(f"Starting invoice processing for: {file_name}")
            result = await self._process_single_invoice(file_bytes, file_type, file_name)
            
            # Log processing result
            if result["success"]:
                logger.debug(f"Successfully processed {file_name}, markdown length: {len(result['markdown'])}")
            else:
                logger.error(f"Failed to process {file_name}: {result['error']}")
            
            # Upload to S3
            logger.debug(f"Starting S3 upload for: {file_name}")
            result = await self._upload_to_s3(result, batch_id, s3_folder)
            
            if "s3_error" in result:
                logger.error(f"S3 upload failed for {file_name}: {result['s3_error']}")
            else:
                logger.debug(f"S3 upload complete for {file_name}: {result.get('s3_key', 'unknown')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Unexpected error in _process_and_upload for {file_name}: {str(e)}")
            logger.error(f"Exception traceback: {traceback.format_exc()}")
            return {
                "success": False,
                "file_name": file_name,
                "error": f"Unhandled exception in _process_and_upload: {str(e)}",
                "timestamp": datetime.datetime.now().isoformat()
            }
    
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
        logger.debug(f"Waiting for rate limiter slot for: {file_name}")
        async with self.rate_limiter:
            logger.debug(f"Rate limiter acquired for: {file_name}")
            try:
                logger.info(f"Processing invoice: {file_name}")
                
                # Open image
                logger.debug(f"Opening image file ({len(image_data)} bytes)")
                try:
                    image = Image.open(BytesIO(image_data))
                    logger.debug(f"Image opened successfully: {image.format}, {image.size}px")
                except Exception as e:
                    logger.error(f"Failed to open image {file_name}: {str(e)}")
                    return {
                        "success": False,
                        "file_name": file_name,
                        "error": f"Image opening error: {str(e)}",
                        "timestamp": datetime.datetime.now().isoformat()
                    }
                
                # Define the prompt for invoice extraction
                prompt = """
                Extract info from the invoice:
                
                Make sure to return only valid markdown.
                """
                
                # Generate content - need to run in thread pool since genai is synchronous
                logger.debug(f"Starting Gemini API call for {file_name}")
                start_time = datetime.datetime.now()
                
                try:
                    response = await asyncio.wait_for(
                        asyncio.to_thread(
                            self.model.generate_content,
                            [prompt, image]
                        ),
                        timeout=120  # 2 minute timeout per invoice
                    )
                    
                    end_time = datetime.datetime.now()
                    processing_time = (end_time - start_time).total_seconds()
                    logger.debug(f"Gemini API call completed in {processing_time:.2f} seconds")
                    
                    # Extract the text from the response
                    markdown_str = response.text.strip()
                    logger.info(f"Successfully processed {file_name} - generated {len(markdown_str)} chars of markdown")
                    
                    return {
                        "success": True,
                        "file_name": file_name,
                        "markdown": markdown_str,
                        "timestamp": datetime.datetime.now().isoformat(),
                        "processing_time_seconds": processing_time
                    }
                except asyncio.TimeoutError:
                    logger.error(f"Timeout processing {file_name} after 120 seconds")
                    return {
                        "success": False,
                        "file_name": file_name,
                        "error": "Gemini API timeout after 120 seconds",
                        "timestamp": datetime.datetime.now().isoformat()
                    }
                except Exception as e:
                    logger.error(f"Gemini API error for {file_name}: {str(e)}")
                    return {
                        "success": False,
                        "file_name": file_name,
                        "error": f"Gemini API error: {str(e)}",
                        "timestamp": datetime.datetime.now().isoformat()
                    }
                
            except Exception as e:
                logger.error(f"Error processing {file_name}: {str(e)}")
                logger.error(f"Exception traceback: {traceback.format_exc()}")
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
        file_name = result["file_name"]
        logger.debug(f"Starting S3 upload for {file_name}, success={result['success']}")
        
        try:
            # Create S3 session with aioboto3
            logger.debug(f"Creating S3 session for {file_name}")
            session = aioboto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key
            )
            
            # Generate a unique filename for the result
            file_base = os.path.splitext(result["file_name"])[0]
            logger.debug(f"Base filename: {file_base}")
            
            async with session.client("s3") as s3:
                if result["success"]:
                    # Upload the markdown content
                    s3_key = f"{s3_folder}{file_base}.md"
                    logger.debug(f"Uploading markdown result to {self.s3_bucket}/{s3_key}")
                    
                    metadata = {
                        "original_filename": result["file_name"],
                        "processing_timestamp": result["timestamp"],
                        "batch_id": batch_id
                    }
                    
                    if "processing_time_seconds" in result:
                        metadata["processing_time_seconds"] = str(result["processing_time_seconds"])
                    
                    logger.debug(f"S3 metadata: {metadata}")
                    
                    await s3.put_object(
                        Bucket=self.s3_bucket,
                        Key=s3_key,
                        Body=result["markdown"].encode('utf-8'),
                        ContentType="text/markdown",
                        Metadata=metadata
                    )
                    logger.info(f"Successfully uploaded markdown for {file_name} to s3://{self.s3_bucket}/{s3_key}")
                else:
                    # Upload error information
                    s3_key = f"{s3_folder}errors/{file_base}.error.txt"
                    logger.debug(f"Uploading error info to {self.s3_bucket}/{s3_key}")
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
                    logger.info(f"Successfully uploaded error info for {file_name} to s3://{self.s3_bucket}/{s3_key}")
                
            result["s3_key"] = s3_key
            result["s3_bucket"] = self.s3_bucket
            return result
            
        except Exception as e:
            logger.error(f"Error uploading {file_name} to S3: {str(e)}")
            logger.error(f"Exception traceback: {traceback.format_exc()}")
            result["s3_error"] = str(e)
            return result

# Usage example
async def process_invoices(
    invoice_files: List[Tuple[bytes, str, str]],
    s3_bucket: str = "invoices-data-dataastra"
) -> Dict[str, Any]:
    """
    Process a batch of invoice files using environment variable credentials
    
    Args:
        invoice_files: List of tuples (file_bytes, file_type, file_name)
        s3_bucket: S3 bucket name
        
    Returns:
        Dict with processing summary
    """
    logger.info(f"Starting process_invoices function with {len(invoice_files)} files")
    logger.info(f"Target S3 bucket: {s3_bucket}")
    
    try:
        processor = InvoiceBatchProcessor(
            s3_bucket=s3_bucket,
            requests_per_minute=10
        )
        
        logger.info("Invoice processor initialized, starting batch processing")
        result = await processor.process_batch(invoice_files)
        logger.info("Batch processing completed successfully")
        return result
        
    except Exception as e:
        logger.critical(f"Critical error in process_invoices: {str(e)}")
        logger.critical(f"Exception traceback: {traceback.format_exc()}")
        raise
