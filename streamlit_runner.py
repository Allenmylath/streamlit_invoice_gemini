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
import pickle

# Import our batch processor
from process_invoice_new import InvoiceBatchProcessor, process_invoices

# Set page configuration
st.set_page_config(
    page_title="Batch Invoice Processor",
    page_icon="📄",
    layout="wide"
)

# File path for storing progress data
PROGRESS_FILE = "/tmp/invoice_progress.pickle"

# Function to save progress data to file
def save_progress(current, total, message):
    try:
        with open(PROGRESS_FILE, 'wb') as f:
            pickle.dump({
                'current': current,
                'total': total,
                'message': message,
                'timestamp': time.time()
            }, f)
    except Exception as e:
        print(f"Error saving progress: {e}")

# Function to load progress data from file
def load_progress():
    try:
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'rb') as f:
                return pickle.load(f)
    except Exception as e:
        print(f"Error loading progress: {e}")
    
    return {
        'current': 0,
        'total': 0,
        'message': 'Initializing...',
        'timestamp': time.time()
    }

# Non-blocking async execution function that doesn't block the main thread
def run_async_non_blocking(coro, callback=None):
    """Run an async coroutine in a non-blocking way."""
    async def _run_and_capture():
        try:
            result = await coro
            if callback:
                callback(result)
            return result
        except Exception as e:
            print(f"Error in async execution: {str(e)}")
            if callback:
                callback({"error": str(e)})
            return {"error": str(e)}
    
    # Create a new thread for the async operation
    thread = threading.Thread(
        target=lambda: asyncio.run(_run_and_capture()),
        daemon=True
    )
    thread.start()
    return thread

# Callback function for when processing completes
def on_processing_complete(result):
    # Mark as complete in session state (safe to modify on main thread)
    if st.session_state.get('processing_started'):
        if "error" in result:
            save_progress(0, 0, f"Error: {result['error']}")
        else:
            save_progress(result['total_processed'], result['total_processed'], "Processing complete!")
        
        # Flag as complete in both session state and a file marker
        with open("/tmp/invoice_processing_complete", "w") as f:
            f.write("complete")

# Modified InvoiceBatchProcessor that reports progress via file
class ProgressReportingProcessor(InvoiceBatchProcessor):
    def __init__(self, s3_bucket="invoices-data-dataastra", requests_per_minute=10):
        super().__init__(s3_bucket, requests_per_minute)
        self.processed_count = 0
        self.total_count = 0
        
    async def process_batch(self, invoice_files):
        # Initialize progress
        self.total_count = len(invoice_files)
        self.processed_count = 0
        save_progress(0, self.total_count, "Starting processing...")
        
        # Call the parent implementation
        return await super().process_batch(invoice_files)
    
    async def _process_and_upload(self, file_bytes, file_type, file_name, batch_id, s3_folder):
        # Call the parent implementation
        result = await super()._process_and_upload(file_bytes, file_type, file_name, batch_id, s3_folder)
        
        # Update progress in file
        self.processed_count += 1
        save_progress(
            self.processed_count, 
            self.total_count,
            f"Processing file {self.processed_count}/{self.total_count}: {file_name}"
        )
        
        return result

# Modified process_invoices function
async def process_invoices_with_progress(
    invoice_files: List[Tuple[bytes, str, str]],
    s3_bucket: str = "invoices-data-dataastra"
) -> dict:
    """Process a batch of invoice files with progress reporting"""
    processor = ProgressReportingProcessor(
        s3_bucket=s3_bucket,
        requests_per_minute=2
    )
    
    # Remove completion marker if it exists
    if os.path.exists("/tmp/invoice_processing_complete"):
        os.remove("/tmp/invoice_processing_complete")
    
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
                
                # Initialize progress
                save_progress(0, len(invoice_files), "Starting processing...")
                
                # Force a rerun to start processing in the other column
                st.rerun()

# Column 2: Processing and Results display
with col2:
    st.subheader("Processing Status")
    
    # Check for completed processing
    processing_complete = os.path.exists("/tmp/invoice_processing_complete")
    if processing_complete and st.session_state.get('processing_started', False):
        st.session_state.processing_complete = True
    
    # If processing has been initiated
    if st.session_state.get('processing_started', False) and not st.session_state.get('processing_complete', False):
        with st.spinner("Processing invoices... This may take several minutes depending on the number of files."):
            # Perform the batch processing
            invoice_files = st.session_state.invoice_files
            
            # Display some initial information
            st.info(f"Processing {len(invoice_files)} invoices")
            
            # Load the current progress
            progress_data = load_progress()
            progress_value = progress_data['current'] / max(progress_data['total'], 1)
            
            # Create progress bar and status text
            progress_bar = st.progress(progress_value)
            status_text = st.empty()
            status_text.text(progress_data['message'])
            
            # Start the processing in a non-blocking way if not already started
            if not st.session_state.get('processing_thread'):
                # Use our modified process_invoices function that reports progress
                st.session_state.processing_thread = run_async_non_blocking(
                    process_invoices_with_progress(invoice_files),
                    callback=on_processing_complete
                )
            
            # Refresh the page automatically every 2 seconds to update progress
            time.sleep(2)
            st.rerun()
    
    # Display results if available
    if st.session_state.get('processing_complete', False):
        progress_data = load_progress()
        
        if "Error" in progress_data.get('message', ''):
            st.error(progress_data['message'])
            st.session_state.processing_started = False
        else:
            st.success("✅ Batch processing complete!")
            
            # Try to load the summary from the session state if available
            summary = None
            
            # If we don't have a summary, display basic information
            if summary is None:
                # Display the final progress
                st.progress(1.0)
                st.info("Processing complete. Check your S3 bucket for results.")
            else:
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
