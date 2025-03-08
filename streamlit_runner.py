# This code needs to be in a separate file named .streamlit/config.toml
# in your project directory to properly set the timeout before Streamlit initializes
"""
[server]
timeout = 1200
maxUploadSize = 200
headless = true
enableCORS = false
enableXsrfProtection = false
"""

# streamlit_runner.py
import streamlit as st
import os
import asyncio
import time
from typing import List, Tuple
from PIL import Image
import io
import uuid
from datetime import datetime
import threading
import json

# Import our batch processor
from process_invoice_new import InvoiceBatchProcessor, process_invoices

# Set page configuration
st.set_page_config(
    page_title="Batch Invoice Processor",
    page_icon="📄",
    layout="wide"
)

# Non-blocking async execution function that doesn't block the main thread
def run_async_non_blocking(coro, callback=None):
    """Run an async coroutine in a non-blocking way.
    
    Args:
        coro: The coroutine to run
        callback: Optional callback function to call with the result
    """
    async def _run_and_capture():
        try:
            result = await coro
            if callback:
                # Use an event loop to safely call the callback from the main thread
                asyncio.run_coroutine_threadsafe(
                    callback_wrapper(result), 
                    asyncio.get_event_loop()
                )
            return result
        except Exception as e:
            st.error(f"Error in async execution: {str(e)}")
            if callback:
                # Call callback with error information
                asyncio.run_coroutine_threadsafe(
                    callback_wrapper({"error": str(e)}), 
                    asyncio.get_event_loop()
                )
            return {"error": str(e)}
            
    async def callback_wrapper(result):
        # This is a coroutine wrapper for the callback
        if callback:
            callback(result)
    
    # Create a new thread for the async operation
    thread = threading.Thread(
        target=lambda: asyncio.run(_run_and_capture()),
        daemon=True
    )
    thread.start()
    return thread

# Callback function for when processing completes
def on_processing_complete(result):
    if "error" in result:
        st.session_state.processing_error = result["error"]
    else:
        st.session_state.summary = result
    
    st.session_state.processing_complete = True
    st.session_state.processing_thread = None
    # Update progress to 100% when done
    if 'progress_bar' in st.session_state:
        st.session_state.progress_bar.progress(1.0)
        st.session_state.status_text.text("Processing complete!")
    st.rerun()

# Function to update progress
def update_progress(current, total):
    """Update the progress bar in the Streamlit app."""
    if 'progress_bar' in st.session_state:
        progress = min(current / total, 1.0) if total > 0 else 0
        st.session_state.progress_bar.progress(progress)
        st.session_state.status_text.text(f"Processing file {current}/{total}...")

# Modified InvoiceBatchProcessor that reports progress
class ProgressReportingProcessor(InvoiceBatchProcessor):
    def __init__(self, s3_bucket="invoices-data-dataastra", requests_per_minute=1000):
        super().__init__(s3_bucket, requests_per_minute)
        self.processed_count = 0
        self.total_count = 0
        
    async def process_batch(self, invoice_files):
        # Set the total count at the beginning
        self.total_count = len(invoice_files)
        self.processed_count = 0
        
        # Start a background task to update progress periodically
        self._start_progress_updates()
        
        # Call the parent implementation
        return await super().process_batch(invoice_files)
    
    async def _process_and_upload(self, file_bytes, file_type, file_name, batch_id, s3_folder):
        # Call the parent implementation
        result = await super()._process_and_upload(file_bytes, file_type, file_name, batch_id, s3_folder)
        
        # Increment the processed count
        self.processed_count += 1
        
        # Update the progress in Streamlit
        # This will execute in a separate thread via streamlit's state
        if hasattr(st, 'session_state') and 'progress_placeholder' in st.session_state:
            update_progress(self.processed_count, self.total_count)
        
        return result
    
    def _start_progress_updates(self):
        # Start a background thread that periodically updates progress
        def update_thread():
            while self.processed_count < self.total_count:
                # Update progress via session state if available
                if hasattr(st, 'session_state') and 'progress_bar' in st.session_state:
                    # Using streamlit's threading-safe operations
                    st.session_state.progress_bar.progress(self.processed_count / self.total_count)
                    st.session_state.status_text.text(f"Processing file {self.processed_count}/{self.total_count}...")
                
                # Sleep for a short period to avoid excessive updates
                time.sleep(0.5)
        
        # Start the thread
        thread = threading.Thread(target=update_thread, daemon=True)
        thread.start()

# Modified process_invoices function that uses our progress-aware processor
async def process_invoices_with_progress(
    invoice_files: List[Tuple[bytes, str, str]],
    s3_bucket: str = "invoices-data-dataastra"
) -> dict:
    """
    Process a batch of invoice files using environment variable credentials
    with progress reporting
    
    Args:
        invoice_files: List of tuples (file_bytes, file_type, file_name)
        s3_bucket: S3 bucket name
        
    Returns:
        Dict with processing summary
    """
    processor = ProgressReportingProcessor(
        s3_bucket=s3_bucket,
        requests_per_minute=2
    )
    
    result = await processor.process_batch(invoice_files)
    return result

# Page title
st.title("Batch Invoice Processing Application")
st.markdown("Upload multiple invoice images (JPG or PNG) and process them using Gemini AI.")

# Create a two-column layout
col1, col2 = st.columns(2)

# Column 1: File upload and processing
with col1:
    st.subheader("Upload Invoices")
    
    # File uploader - allow multiple files, only allow jpg and png
    uploaded_files = st.file_uploader("Choose invoice images", 
                                    type=["jpg", "jpeg", "png"],
                                    accept_multiple_files=True)
    
    # Show warning about rate limits
    st.warning("⚠️ Processing capacity: 10 invoices per minute due to API rate limits")
    
    # Process button
    if uploaded_files and st.button("Process Invoices"):
        # Check API keys
        if not os.environ.get("GEMINI_API_KEY"):
            st.error("GEMINI_API_KEY environment variable not set")
        else:
            # Prepare the list of files
            invoice_files = []
            for file in uploaded_files:
                file_bytes = file.getvalue()
                file_type = file.type
                file_name = file.name
                
                invoice_files.append((file_bytes, file_type, file_name))
            
            # Store files in session state
            if invoice_files:
                st.session_state.invoice_files = invoice_files
                st.session_state.processing_started = True
                st.session_state.processing_complete = False
                st.session_state.summary = None
                st.session_state.processing_error = None
                
                # Force a rerun to start processing in the other column
                st.rerun()

# Column 2: Processing and Results display
with col2:
    st.subheader("Processing Status")
    
    # If processing has been initiated
    if st.session_state.get('processing_started', False) and not st.session_state.get('processing_complete', False):
        with st.spinner("Processing invoices... This may take several minutes depending on the number of files."):
            # Perform the batch processing
            invoice_files = st.session_state.invoice_files
            
            # Display some initial information
            st.info(f"Processing {len(invoice_files)} invoices")
            
            # Create progress bar and status text
            st.session_state.progress_bar = st.progress(0)
            st.session_state.status_text = st.empty()
            
            # Store a reference to the progress placeholder for the background thread
            st.session_state.progress_placeholder = True
            
            # Start the processing in a non-blocking way if not already started
            if not st.session_state.get('processing_thread'):
                # Use our modified process_invoices function that reports progress
                st.session_state.processing_thread = run_async_non_blocking(
                    process_invoices_with_progress(invoice_files),
                    callback=on_processing_complete
                )
                
                st.session_state.status_text.text("Processing started in background...")
    
    # Display results if available
    if st.session_state.get('processing_complete', False):
        if st.session_state.get('processing_error'):
            st.error(f"Error during processing: {st.session_state.processing_error}")
            st.session_state.processing_started = False
        
        elif st.session_state.get('summary'):
            summary = st.session_state.summary
            
            st.success("✅ Batch processing complete!")
            
            # Show batch information
            st.markdown("### Batch Information")
            st.write(f"**Batch ID:** {summary['batch_id']}")
            st.write(f"**Timestamp:** {summary['timestamp']}")
            st.write(f"**S3 Location:** {summary['s3_folder']}")
            
            # Show statistics
            st.markdown("### Processing Statistics")
            col_stats1, col_stats2, col_stats3 = st.columns(3)
            
            with col_stats1:
                st.metric(label="Total Files", value=summary['total_files'])
            
            with col_stats2:
                st.metric(label="Successfully Processed", value=summary['successful'])
            
            with col_stats3:
                st.metric(label="Failed", value=summary['failed'])
            
            # Show detailed file status
            st.markdown("### File Status")
            
            # Create a table to display file results
            file_data = []
            for file_info in summary['files']:
                status = "✅ Success" if file_info['success'] else "❌ Failed"
                s3_key = file_info.get('s3_key', 'N/A')
                file_data.append({
                    "File Name": file_info['file_name'],
                    "Status": status,
                    "S3 Key": s3_key
                })
            
            # Display the file status table
            st.dataframe(file_data, use_container_width=True)

# Add some helpful information at the bottom
st.markdown("---")
st.markdown("### How to use this app")
st.markdown("""
1. Upload multiple invoice images (JPG or PNG)
2. Click 'Process Invoices' to extract information from all invoices
3. The app will process files at a rate of 10 per minute (Gemini API limit)
4. Results will be stored in S3 and a summary will be displayed
""")

# Add a footer
st.markdown("---")
st.markdown("Batch Invoice Processing App powered by Gemini AI")

# Initialize session state if not already done
if 'processing_started' not in st.session_state:
    st.session_state.processing_started = False

if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False

if 'processing_thread' not in st.session_state:
    st.session_state.processing_thread = None

if 'summary' not in st.session_state:
    st.session_state.summary = None

if 'processing_error' not in st.session_state:
    st.session_state.processing_error = None
