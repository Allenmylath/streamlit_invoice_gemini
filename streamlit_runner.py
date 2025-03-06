import streamlit as st
import os
from PIL import Image
import io
from process_invoice import process_invoice

# Set page configuration
st.set_page_config(
    page_title="Invoice Processor",
    page_icon="📄",
    layout="wide"
)

# Page title
st.title("Invoice Processing Application")
st.markdown("Upload an invoice image (JPG or PNG) and process it using Gemini AI.")

# Create a two-column layout
col1, col2 = st.columns(2)

# Column 1: File upload and processing
with col1:
    st.subheader("Upload Invoice")
    
    # File uploader - only allow jpg and png files
    uploaded_file = st.file_uploader("Choose an invoice image", type=["jpg", "jpeg", "png"])
    
    # Display uploaded file
    if uploaded_file is not None:
        file_type = uploaded_file.type
        
        # Display the uploaded image
        st.image(uploaded_file, caption="Uploaded Invoice", use_column_width=True)
        
        # Get the API key from environment variable
        api_key = os.environ.get("GEMINI_API_KEY")
        
        # Process button
        if st.button("Process Invoice"):
            if api_key:
                with st.spinner("Processing invoice..."):
                    # Read file bytes
                    file_bytes = uploaded_file.getvalue()
                    
                    # Process the invoice
                    result = process_invoice(api_key, file_bytes, file_type)
                    
                    # Store the result in session state to display in the other column
                    st.session_state.markdown_result = result
                    
                    # Force a rerun to update the other column
                    st.rerun()
            else:
                st.error("GEMINI_API_KEY environment variable not set")

# Column 2: Results display
with col2:
    st.subheader("Extracted Information")
    
    # Create a placeholder for results
    result_placeholder = st.empty()
    
    # Display results if available
    if 'markdown_result' in st.session_state:
        with result_placeholder.container():
            st.markdown("### Extracted Data")
            st.markdown(st.session_state.markdown_result)
            
            # Option to download the results
            st.download_button(
                label="Download Results",
                data=st.session_state.markdown_result,
                file_name="invoice_data.md",
                mime="text/markdown"
            )

# Add some helpful information at the bottom
st.markdown("---")
st.markdown("### How to use this app")
st.markdown("""
1. Upload an invoice image (JPG or PNG only)
2. Click 'Process Invoice' to extract the information
3. View the extracted data in markdown format
4. Download the results if needed
""")

# Add a footer
st.markdown("---")
st.markdown("Invoice Processing App powered by Gemini AI")
