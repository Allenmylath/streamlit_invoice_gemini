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

# Configure logging with focus on errors and important events
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("InvoiceBatchProcessor")

class InvoiceBatchProcessor:
    def __init__(
        self,
        s3_bucket: str = "invoices-data-dataastra",
        requests_per_minute: int = 1000
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
            raise ValueError("GEMINI_API_KEY environment variable is required")
        
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            logger.warning("AWS credentials may be missing from environment variables")
        
        self.s3_bucket = s3_bucket
        
        # Create a rate limiter
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
        
        print(f"[BATCH START] Processing batch {batch_id} with {len(invoice_files)} files")
        logger.info(f"Starting batch processing of {len(invoice_files)} invoices. Batch ID: {batch_id}")
        
        # Upload batch start marker to S3
        try:
            session = aioboto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key
            )
            
            async with session.client("s3") as s3:
                batch_start_key = f"{s3_folder}_batch_start.txt"
                start_info = f"Batch {batch_id} started at {timestamp}\nFiles to process: {len(invoice_files)}"
                await s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=batch_start_key,
                    Body=start_info.encode('utf-8'),
                    ContentType="text/plain"
                )
                print(f"[BATCH MARKER] Created start marker in S3: {batch_start_key}")
        except Exception as e:
            logger.warning(f"Failed to upload batch start marker: {str(e)}")
            print(f"[BATCH MARKER ERROR] {str(e)}")
        
        # Process files concurrently with rate limiting
        tasks = []
        skipped_count = 0
        
        for idx, (file_bytes, file_type, file_name) in enumerate(invoice_files):
            # Skip non-image files
            if not file_type.startswith('image/'):
                logger.warning(f"Skipping non-image file: {file_name} with type {file_type}")
                skipped_count += 1
                continue
                
            # Create task for processing and uploading (each file gets processed and uploaded immediately)
            task = asyncio.create_task(
                self._process_and_upload(file_bytes, file_type, file_name, batch_id, s3_folder)
            )
            tasks.append(task)
        
        if skipped_count > 0:
            logger.warning(f"Skipped {skipped_count} non-image files")
        
        logger.info(f"Processing {len(tasks)} invoice images...")
        print(f"[BATCH PROCESSING] Processing {len(tasks)} images (skipped {skipped_count} non-images)")
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions that might have been returned
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task {i} failed with exception: {str(result)}")
                print(f"[TASK ERROR] Task {i} failed: {str(result)}")
                
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
        print(f"[BATCH COMPLETE] {successful} successful, {failed} failed")
        
        # Create a summary file
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
        print(f"[SUMMARY UPLOAD] Uploading final batch summary to S3")
        try:
            session = aioboto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key
            )
            
            async with session.client("s3") as s3:
                summary_key = f"{s3_folder}_summary.json"
                await s3.put_object(
                    Bucket=self.s3_bucket,
                    Key=summary_key,
                    Body=str(summary).encode('utf-8'),
                    ContentType="application/json"
                )
                logger.info(f"Summary uploaded to s3://{self.s3_bucket}/{summary_key}")
                print(f"[SUMMARY UPLOAD SUCCESS] Summary uploaded to {summary_key}")
        except Exception as e:
            logger.error(f"Error uploading summary to S3: {str(e)}")
            logger.error(f"Exception traceback: {traceback.format_exc()}")
            print(f"[SUMMARY UPLOAD ERROR] Failed to upload summary: {str(e)}")
            
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
        """
        try:
            # Process the invoice with rate limiting
            result = await self._process_single_invoice(file_bytes, file_type, file_name)
            
            # Upload to S3 immediately after processing
            print(f"[S3 UPLOAD START] Uploading result for {file_name}")
            result = await self._upload_to_s3(result, batch_id, s3_folder)
            
            if "s3_error" in result:
                logger.error(f"S3 upload failed for {file_name}: {result['s3_error']}")
                print(f"[S3 UPLOAD ERROR] Failed for {file_name}: {result['s3_error']}")
            else:
                print(f"[S3 UPLOAD SUCCESS] Completed for {file_name}: {result.get('s3_key')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Unexpected error processing {file_name}: {str(e)}")
            logger.error(f"Exception traceback: {traceback.format_exc()}")
            print(f"[PROCESSING ERROR] {file_name} failed with error: {str(e)}")
            
            # Try to upload the error immediately
            error_result = {
                "success": False,
                "file_name": file_name,
                "error": f"Unhandled exception: {str(e)}",
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            try:
                error_result = await self._upload_to_s3(error_result, batch_id, s3_folder)
                print(f"[S3 ERROR UPLOAD] Error report for {file_name} uploaded")
            except Exception as s3_e:
                print(f"[S3 ERROR UPLOAD FAILED] Could not upload error for {file_name}: {str(s3_e)}")
                
            return error_result
    
    async def _process_single_invoice(
        self, 
        image_data: bytes, 
        file_type: str, 
        file_name: str
    ) -> Dict[str, Any]:
        """
        Process a single invoice with rate limiting
        """
        # Apply rate limiting
        async with self.rate_limiter:
            try:
                logger.info(f"Processing invoice: {file_name}")
                
                # Open image
                try:
                    image = Image.open(BytesIO(image_data))
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
                start_time = datetime.datetime.now()
                
                try:
                    # Explicitly print before and after the Gemini API call to track issues
                    print(f"[GEMINI API START] Processing {file_name} at {start_time.isoformat()}")
                    
                    response = await asyncio.wait_for(
                        asyncio.to_thread(
                            self.model.generate_content,
                            [prompt, image]
                        ),
                        timeout=120  # 2 minute timeout per invoice
                    )
                    
                    end_time = datetime.datetime.now()
                    processing_time = (end_time - start_time).total_seconds()
                    print(f"[GEMINI API SUCCESS] Completed {file_name} in {processing_time:.2f}s")
                    
                    # Extract the text from the response
                    markdown_str = response.text.strip()
                    logger.info(f"Successfully processed {file_name} - generated {len(markdown_str)} chars")
                    
                    return {
                        "success": True,
                        "file_name": file_name,
                        "markdown": markdown_str,
                        "timestamp": datetime.datetime.now().isoformat(),
                        "processing_time_seconds": processing_time
                    }
                except asyncio.TimeoutError:
                    print(f"[GEMINI API TIMEOUT] {file_name} timed out after 120 seconds")
                    logger.error(f"Timeout processing {file_name} after 120 seconds")
                    return {
                        "success": False,
                        "file_name": file_name,
                        "error": "Gemini API timeout after 120 seconds",
                        "timestamp": datetime.datetime.now().isoformat()
                    }
                except Exception as e:
                    print(f"[GEMINI API ERROR] {file_name} failed with error: {str(e)}")
                    print(f"[GEMINI API ERROR TRACE] {traceback.format_exc()}")
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
        """
        file_name = result["file_name"]
        
        try:
            # Create S3 session with aioboto3
            session = aioboto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key
            )
            
            # Generate a unique filename for the result
            file_base = os.path.splitext(result["file_name"])[0]
            
            # Add timestamp to ensure uniqueness even with same filename
            timestamp_short = datetime.datetime.now().strftime("%H%M%S")
            
            async with session.client("s3") as s3:
                if result["success"]:
                    # Upload the markdown content
                    s3_key = f"{s3_folder}{file_base}_{timestamp_short}.md"
                    
                    metadata = {
                        "original_filename": result["file_name"],
                        "processing_timestamp": result["timestamp"],
                        "batch_id": batch_id
                    }
                    
                    if "processing_time_seconds" in result:
                        metadata["processing_time_seconds"] = str(result["processing_time_seconds"])
                    
                    try:
                        await s3.put_object(
                            Bucket=self.s3_bucket,
                            Key=s3_key,
                            Body=result["markdown"].encode('utf-8'),
                            ContentType="text/markdown",
                            Metadata=metadata
                        )
                        logger.info(f"Uploaded markdown for {file_name} to {s3_key}")
                    except Exception as s3_e:
                        logger.error(f"S3 upload error for {file_name}: {str(s3_e)}")
                        print(f"[S3 PUT ERROR] Failed to upload {file_name}: {str(s3_e)}")
                        print(f"[S3 PUT ERROR TRACE] {traceback.format_exc()}")
                        raise  # Re-raise to be caught by outer try/except
                else:
                    # Upload error information
                    s3_key = f"{s3_folder}errors/{file_base}_{timestamp_short}.error.txt"
                    try:
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
                        logger.info(f"Uploaded error info for {file_name} to {s3_key}")
                    except Exception as s3_e:
                        logger.error(f"S3 error upload failed for {file_name}: {str(s3_e)}")
                        print(f"[S3 ERROR PUT ERROR] Failed to upload error for {file_name}: {str(s3_e)}")
                        print(f"[S3 ERROR PUT TRACE] {traceback.format_exc()}")
                        raise  # Re-raise to be caught by outer try/except
                
            result["s3_key"] = s3_key
            result["s3_bucket"] = self.s3_bucket
            return result
            
        except Exception as e:
            logger.error(f"Error uploading {file_name} to S3: {str(e)}")
            print(f"[S3 UPLOAD FAILED] {file_name}: {str(e)}")
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
    
    try:
        processor = InvoiceBatchProcessor(
            s3_bucket=s3_bucket,
            requests_per_minute=100
        )
        
        result = await processor.process_batch(invoice_files)
        return result
        
    except Exception as e:
        logger.critical(f"Critical error in process_invoices: {str(e)}")
        logger.critical(f"Exception traceback: {traceback.format_exc()}")
        raise
