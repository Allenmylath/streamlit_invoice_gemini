# streamlit_runner.py
import streamlit as st
import os
import asyncio
import time
from typing import List, Tuple
from PIL import Image
import io
import uuid
from datetime import datetime, timedelta
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

# File paths for storing progress data and summary
PROGRESS_FILE = "/tmp/invoice_progress.pickle"
SUMMARY_FILE = "/tmp/invoice_summary.json"
COMPLETE_FILE = "/tmp/invoice_processing_complete"
TIMER_FILE = "/tmp/invoice_timer.json"

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

# Function to save summary to file - avoids session state
def save_summary(summary):
    try:
        with open(SUMMARY_FILE, 'w') as f:
            json.dump(summary, f)
    except Exception as e:
        print(f"Error saving summary: {e}")

# Function to load summary from file
def load_summary():
    try:
        if os.path.exists(SUMMARY_FILE):
            with open(SUMMARY_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading summary: {e}")
    return None

# Function to save timer data
def save_timer(start_time, end_time=None):
    try:
        with open(TIMER_FILE, 'w') as f:
            json.dump({
                'start_time': start_time,
                'end_time': end_time,
            }, f)
    except Exception as e:
        print(f"Error saving timer: {e}")

# Function to load timer data
def load_timer():
    try:
        if os.path.exists(TIMER_FILE):
            with open(TIMER_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading timer: {e}")
    return {'start_time': None, 'end_time': None}

# Format time difference in a human-readable format
def format_time_elapsed(start_time, end_time=None):
    if not start_time:
        return "00:00:00"
    
    start = datetime.fromtimestamp(start_time)
    
    if end_time:
        end = datetime.fromtimestamp(end_time)
    else:
        end = datetime.now()
    
    diff = end - start
    
    # Format as HH:MM:SS
    hours, remainder = divmod(diff.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

# Estimate time remaining based on progress
def estimate_time_remaining(start_time, current, total):
    if current <= 0 or not start_time:
        return "Calculating..."
    
    elapsed_seconds = time.time() - start_time
    items_per_second = current / elapsed_seconds
    
    if items_per_second <= 0:
        return "Calculating..."
    
    remaining_items = total - current
    remaining_seconds = remaining_items / items_per_second
    
    # Format remaining time
    if remaining_seconds < 60:
        return f"{int(remaining_seconds)} seconds"
    elif remaining_seconds < 3600:
        minutes = remaining_seconds / 60
        return f"{int(minutes)} minutes"
    else:
        hours = remaining_seconds / 3600
        minutes = (remaining_seconds % 3600) / 60
        return f"{int(hours)} hours, {int(minutes)} minutes"

# Non-blocking async execution function that doesn't block the main thread
def run_async_non_blocking(coro):
    """Run an async coroutine in a non-blocking way."""
    async def _run_and_capture():
        try:
            # Record the start time
            start_time = time.time()
            save_timer(start_time)
            
            # Process the invoices
            result = await coro
            
            # Record the end time
            end_time = time.time()
            save_timer(start_time, end_time)
            
            # Save result to file instead of using callback
            save_summary(result)
            
            # Create completion marker
            with open(COMPLETE_FILE, "w") as f:
                f.write("complete")
            
            # Update progress to complete
            save_progress(
                result.get('total_processed', 0), 
                result.get('total_files', 0),
                "Processing complete!"
            )
            return result
        except Exception as e:
            print(f"Error in async execution: {str(e)}")
            # Save error to progress
            save_progress(0, 0, f"Error: {str(e)}")
            # Create error marker
            with open(COMPLETE_FILE, "w") as f:
                f.write("error")
            return {"error": str(e)}
    
    # Create a new thread for the async operation
    thread = threading.Thread(
        target=lambda: asyncio.run(_run_and_capture()),
        daemon=True
    )
    thread.start()
    return thread

# Modified InvoiceBatchProcessor that reports progress via file
class ProgressReportingProcessor(InvoiceBatchProcessor):
    def __init__(self, s3_bucket="invoices-data-dataastra", requests_per_sec=25):
        super().__init__(s3_bucket, requests_per_sec)
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
        requests_per_sec=25
    )
    
    # Remove completion marker and summary if they exist
    if os.path.exists(COMPLETE_FILE):
        os.remove(COMPLETE_FILE)
    if os.path.exists(SUMMARY_FILE):
        os.remove(SUMMARY_FILE)
    
    result = await processor.process_batch(invoice_files)
    return result

# Function to reset processing state
def reset_processing_state():
    # Reset session state
    st.session_state.processing_started = False
    st.session_state.processing_complete = False
    st.session_state.processing_thread = None
    
    # Clean up files
    for file_path in [PROGRESS_FILE, SUMMARY_FILE, COMPLETE_FILE, TIMER_FILE]:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Error removing file {file_path}: {e}")

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
    
    # Add a reset button
    reset_col, process_col = st.columns(2)
    
    with reset_col:
        if st.button("Reset"):
            reset_processing_state()
            st.rerun()
    
    # Process button
    with process_col:
        process_button = st.button("Process Invoices")
        
    if uploaded_files and process_button:
        # Reset processing state first to ensure fresh start
        reset_processing_state()
        
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
            
            # Store files in session state and start processing
            if invoice_files:
                st.session_state.invoice_files = invoice_files
                st.session_state.processing_started = True
                st.session_state.processing_complete = False
                
                # Initialize timer
                save_timer(time.time())
                
                # Initialize progress
                save_progress(0, len(invoice_files), "Starting processing...")
                
                # Force a rerun to start processing in the other column
                st.rerun()

# Column 2: Processing and Results display
with col2:
    st.subheader("Processing Status")
    
    # Check for completed processing
    processing_complete = os.path.exists(COMPLETE_FILE)
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
            total = max(progress_data['total'], 1)  # Avoid division by zero
            progress_value = min(progress_data['current'] / total, 1.0)  # Ensure doesn't exceed 1.0
            
            # Load the timer data
            timer_data = load_timer()
            start_time = timer_data.get('start_time')
            
            # Display the timer
            elapsed_time = format_time_elapsed(start_time)
            remaining_time = estimate_time_remaining(start_time, progress_data['current'], total)
            
            # Create timer display
            timer_col1, timer_col2 = st.columns(2)
            with timer_col1:
                st.metric("Time Elapsed", elapsed_time)
            with timer_col2:
                st.metric("Estimated Remaining", remaining_time)
            
            # Create progress bar and status text
            progress_bar = st.progress(progress_value)
            status_text = st.empty()
            status_text.text(progress_data['message'])
            
            # Start the processing in a non-blocking way if not already started
            if not st.session_state.get('processing_thread'):
                # Use our modified process_invoices function that reports progress
                st.session_state.processing_thread = run_async_non_blocking(
                    process_invoices_with_progress(invoice_files)
                )
            
            # Refresh the page automatically to update progress
            time.sleep(1)
            st.rerun()
    
    # Display results if available
    if st.session_state.get('processing_complete', False):
        # Load the final progress data
        progress_data = load_progress()
        
        # Load timer data
        timer_data = load_timer()
        start_time = timer_data.get('start_time')
        end_time = timer_data.get('end_time', time.time())
        total_time = format_time_elapsed(start_time, end_time)
        
        # Display the total processing time
        st.metric("Total Processing Time", total_time)
        
        # Check if there was an error
        if "Error" in progress_data.get('message', ''):
            st.error(progress_data['message'])
            st.session_state.processing_started = False
        else:
            st.success("✅ Batch processing complete!")
            
            # Load the summary from file
            summary = load_summary()
            
            if summary is None:
                # Display the final progress without summary details
                st.progress(1.0)
                st.info("Processing complete. Check your S3 bucket for results.")
            else:
                # Show batch information
                st.markdown("### Batch Information")
                
                # Get S3 location and create a clickable link
                batch_id = summary.get('batch_id', 'N/A')
                timestamp = summary.get('timestamp', 'N/A')
                s3_folder = summary.get('s3_folder', 'N/A')
                s3_bucket = summary.get('s3_bucket', 'invoices-data-dataastra')
                
                st.write(f"**Batch ID:** {batch_id}")
                st.write(f"**Timestamp:** {timestamp}")
                
                # Create an AWS S3 console URL (assuming AWS console access)
                s3_console_url = f"https://s3.console.aws.amazon.com/s3/buckets/{s3_bucket}?region=us-east-1&prefix={s3_folder}"
                
                # Display both the folder path and a clickable link
                st.write(f"**S3 Location:** {s3_folder}")
                st.markdown(f"[View in S3 Console]({s3_console_url}) (requires AWS console access)")
                
                # Show statistics
                st.markdown("### Processing Statistics")
                col_stats1, col_stats2, col_stats3 = st.columns(3)
                
                with col_stats1:
                    st.metric(label="Total Files", value=summary.get('total_files', 0))
                
                with col_stats2:
                    st.metric(label="Successfully Processed", value=summary.get('successful', 0))
                
                with col_stats3:
                    st.metric(label="Failed", value=summary.get('failed', 0))
                
                # Show average time per file
                if start_time and end_time and summary.get('total_processed', 0) > 0:
                    total_seconds = end_time - start_time
                    avg_seconds_per_file = total_seconds / summary.get('total_processed', 1)
                    
                    if avg_seconds_per_file < 60:
                        avg_time = f"{avg_seconds_per_file:.1f} seconds"
                    else:
                        avg_time = f"{(avg_seconds_per_file/60):.1f} minutes"
                    
                    st.metric(label="Average Time Per File", value=avg_time)
                
                # Show detailed file status if files data is available
                if 'files' in summary:
                    st.markdown("### File Status")
                    
                    # Create a table to display file results
                    file_data = []
                    for file_info in summary['files']:
                        status = "✅ Success" if file_info.get('success', False) else "❌ Failed"
                        s3_key = file_info.get('s3_key', 'N/A')
                        
                        # Create direct link to the file in S3 if file was successful
                        if file_info.get('success', False) and s3_key != 'N/A':
                            s3_file_url = f"https://s3.console.aws.amazon.com/s3/object/{s3_bucket}?region=us-east-1&prefix={s3_key}"
                            s3_link = f"[View in S3]({s3_file_url})"
                        else:
                            s3_link = "N/A"
                        
                        file_data.append({
                            "File Name": file_info.get('file_name', 'Unknown'),
                            "Status": status,
                            "S3 Key": s3_key,
                            "Link": s3_link
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
5. Click 'Reset' to process a new batch of invoices
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
