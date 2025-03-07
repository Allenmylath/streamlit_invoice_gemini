import streamlit as st
import os
import asyncio
import time
from typing import List, Tuple
from PIL import Image
import io
import uuid
from datetime import datetime

# Import our batch processor
from process_invoice_new import InvoiceBatchProcessor, process_invoices

# Set page configuration
st.set_page_config(
    page_title="Batch Invoice Processor",
    page_icon="📄",
    layout="wide"
)

# Helper function to run async functions in Streamlit
def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

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
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Process the batch
            try:
                # This is a blocking operation - Streamlit will show the spinner until complete
                summary = run_async(process_invoices(invoice_files))
                
                # Store the results and mark processing as complete
                st.session_state.summary = summary
                st.session_state.processing_complete = True
                
                # Update the progress bar to show completion
                progress_bar.progress(100)
                status_text.success("Processing complete!")
                
                # Force a rerun to display the results
                st.rerun()
            except Exception as e:
                st.error(f"Error during processing: {str(e)}")
                st.session_state.processing_started = False
    
    # Display results if available
    if st.session_state.get('processing_complete', False) and st.session_state.get('summary'):
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

if 'summary' not in st.session_state:
    st.session_state.summary = None
