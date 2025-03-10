# pages/1_S3_Browser.py
import streamlit as st
import boto3
import botocore
import os
import pandas as pd
from datetime import datetime
import io
import dotenv

# Load environment variables if .env exists
dotenv.load_dotenv()

# Set page configuration
st.set_page_config(
    page_title="Markdown Browser | Batch Invoice Processor",
    page_icon="📄",
    layout="wide"
)

# Add custom CSS to enhance folders and MD files display
st.markdown("""
<style>
    .folder-container {
        padding: 12px;
        background-color: #f8f9fa;
        border-radius: 10px;
        margin-bottom: 20px;
        border: 2px solid #dfe3e8;
    }
    .folder-item {
        padding: 12px;
        border-radius: 8px;
        background-color: #ffe8b3;
        margin-bottom: 8px;
        border-left: 5px solid #ffb703;
        font-weight: 600;
        font-size: 16px;
    }
    .md-file-item {
        padding: 10px;
        border-radius: 8px;
        background-color: #e3f2fd;
        margin-bottom: 6px;
        border-left: 5px solid #2196f3;
    }
    .breadcrumb {
        padding: 10px 15px;
        background-color: #f5f7f9;
        border-radius: 8px;
        margin-bottom: 20px;
    }
    .preview-container {
        padding: 15px;
        background-color: #ffffff;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
    }
</style>
""", unsafe_allow_html=True)

# Page header
st.title("Markdown Files Browser")
st.markdown("Browse and preview markdown files from your S3 bucket")
st.markdown("---")

# Initialize session state variables
if 'current_path' not in st.session_state:
    st.session_state.current_path = ""
if 'selected_file' not in st.session_state:
    st.session_state.selected_file = None
if 'history' not in st.session_state:
    st.session_state.history = []
if 'history_index' not in st.session_state:
    st.session_state.history_index = -1

# Default bucket name (from your invoice processor)
DEFAULT_BUCKET = "output-json-invoice"

# Sidebar for settings
with st.sidebar:
    st.header("Browser Settings")
    
    # Option to change bucket
    bucket_name = st.text_input("S3 Bucket Name", value=DEFAULT_BUCKET)
    
    # Authentication options
    auth_method = st.radio(
        "Authentication Method",
        ["AWS Environment Variables", "AWS Profile", "IAM Role", "No Authentication"]
    )
    
    if auth_method == "AWS Environment Variables":
        st.info("Using AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from environment")
    elif auth_method == "AWS Profile":
        profile_name = st.text_input("AWS Profile Name", value="default")
    
    st.markdown("---")
    st.markdown("### Navigation Controls")
    
    # Back and forward buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("⬅️ Back", disabled=st.session_state.history_index <= 0):
            st.session_state.history_index -= 1
            st.session_state.current_path = st.session_state.history[st.session_state.history_index]
            st.rerun()
    
    with col2:
        if st.button("➡️ Forward", disabled=st.session_state.history_index >= len(st.session_state.history) - 1):
            st.session_state.history_index += 1
            st.session_state.current_path = st.session_state.history[st.session_state.history_index]
            st.rerun()
    
    # Home button
    if st.button("🏠 Home"):
        # Add to history if changing path
        if st.session_state.current_path != "":
            if st.session_state.history_index < len(st.session_state.history) - 1:
                # Cut the history at current index if we're not at the end
                st.session_state.history = st.session_state.history[:st.session_state.history_index + 1]
            st.session_state.history.append("")
            st.session_state.history_index = len(st.session_state.history) - 1
        
        st.session_state.current_path = ""
        st.session_state.selected_file = None
        st.rerun()

# Function to get s3 client based on authentication method
def get_s3_client():
    try:
        if auth_method == "AWS Environment Variables":
            # Use environment variables automatically
            return boto3.client('s3')
        
        elif auth_method == "AWS Profile":
            # Use a specific profile
            session = boto3.Session(profile_name=profile_name)
            return session.client('s3')
        
        elif auth_method == "IAM Role":
            # Use IAM role (typically for EC2/Lambda)
            return boto3.client('s3')
        
        else:  # No Authentication
            # Try with no auth - may work for some public-read buckets
            return boto3.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))
    
    except Exception as e:
        st.sidebar.error(f"Error connecting to S3: {str(e)}")
        return None

# Function to list objects in a bucket with a prefix
def list_objects(bucket, prefix=""):
    s3_client = get_s3_client()
    if not s3_client:
        return [], []
    
    # Handle trailing slash for directories
    if prefix and not prefix.endswith('/') and prefix:
        prefix = prefix + '/'
    
    # List objects in the bucket/prefix
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        
        # Extract folders (common prefixes)
        folders = []
        if 'CommonPrefixes' in response:
            for obj in response['CommonPrefixes']:
                folder_name = obj['Prefix']
                # Remove the current prefix to get just the folder name
                if prefix:
                    folder_name = folder_name.replace(prefix, '', 1)
                # Remove trailing slash
                if folder_name.endswith('/'):
                    folder_name = folder_name[:-1]
                folders.append({
                    'name': folder_name,
                    'full_path': obj['Prefix'],
                    'type': 'folder'
                })
        
        # Extract only markdown files
        md_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                # Skip the directory itself or objects that represent directories
                key = obj['Key']
                if key == prefix or key.endswith('/'):
                    continue
                
                # Only include .md files
                if not key.lower().endswith('.md'):
                    continue
                
                # Remove the current prefix to get just the file name
                file_name = key
                if prefix:
                    file_name = key.replace(prefix, '', 1)
                
                # Get file size in human-readable format
                size_bytes = obj['Size']
                if size_bytes < 1024:
                    size_str = f"{size_bytes} B"
                elif size_bytes < 1024 * 1024:
                    size_str = f"{size_bytes / 1024:.1f} KB"
                else:
                    size_str = f"{size_bytes / (1024 * 1024):.1f} MB"
                
                md_files.append({
                    'name': file_name,
                    'full_path': key,
                    'size': size_str,
                    'last_modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                })
        
        return folders, md_files
    
    except Exception as e:
        st.error(f"Error accessing S3 bucket: {str(e)}")
        return [], []

# Function to get file content
def get_file_content(bucket, key):
    s3_client = get_s3_client()
    if not s3_client:
        return None
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except Exception as e:
        st.error(f"Error reading file: {str(e)}")
        return None

# Function to get a pre-signed URL for download (works better for authenticated buckets)
def get_presigned_url(bucket, key, expiration=3600):
    s3_client = get_s3_client()
    if not s3_client:
        return None
    
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expiration
        )
        return url
    except Exception as e:
        st.error(f"Error creating presigned URL: {str(e)}")
        return None

# Function to navigate to a folder
def navigate_to_folder(folder_path):
    # Add current path to history before navigating if it's different
    if st.session_state.current_path != folder_path:
        if st.session_state.history_index < len(st.session_state.history) - 1:
            # Cut the history at current index if we're not at the end
            st.session_state.history = st.session_state.history[:st.session_state.history_index + 1]
        
        st.session_state.history.append(folder_path)
        st.session_state.history_index = len(st.session_state.history) - 1
        
    st.session_state.current_path = folder_path
    st.session_state.selected_file = None
    st.rerun()

# Create breadcrumb navigation
def render_breadcrumb():
    st.markdown('<div class="breadcrumb">', unsafe_allow_html=True)
    
    # Create columns for breadcrumb parts
    parts = st.session_state.current_path.split('/')
    parts = [p for p in parts if p]  # Remove empty strings
    
    cols = st.columns(len(parts) + 1)
    
    # Home button
    with cols[0]:
        if st.button("🏠 Root", key="home_crumb"):
            navigate_to_folder("")
    
    # Path parts
    path_so_far = ""
    for i, part in enumerate(parts):
        path_so_far += part + "/"
        with cols[i + 1]:
            if st.button(part, key=f"crumb_{i}"):
                navigate_to_folder(path_so_far)
    
    st.markdown('</div>', unsafe_allow_html=True)

# Main application
try:
    # Show breadcrumb navigation
    if st.session_state.current_path:
        st.markdown(f"**Current Location:** `{st.session_state.current_path}`")
        render_breadcrumb()
    else:
        st.markdown("**Current Location:** `Root`")
    
    # Create two columns - file browser and preview
    browser_col, preview_col = st.columns([3, 2])
    
    with browser_col:
        # List objects in current path
        folders, md_files = list_objects(bucket_name, st.session_state.current_path)
        
        # Display folders with clear visual distinction
        if folders:
            st.markdown('<div class="folder-container">', unsafe_allow_html=True)
            st.subheader("📁 FOLDERS")
            
            # Create a grid of folder buttons for better visibility
            folder_count = len(folders)
            cols_per_row = 1  # Display one folder per row for clarity
            
            for i in range(0, folder_count, cols_per_row):
                cols = st.columns(cols_per_row)
                for j in range(cols_per_row):
                    idx = i + j
                    if idx < folder_count:
                        folder = folders[idx]
                        with cols[j % cols_per_row]:
                            st.markdown(f'<div class="folder-item">📁 {folder["name"]}</div>', unsafe_allow_html=True)
                            if st.button("Open Folder", key=f"folder_{idx}"):
                                navigate_to_folder(folder['full_path'])
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Display markdown files
        if md_files:
            st.subheader("📝 MARKDOWN FILES")
            
            # File search/filter
            search_term = st.text_input("Search markdown files:", placeholder="Type to search...")
            
            filtered_files = md_files
            if search_term:
                filtered_files = [f for f in md_files if search_term.lower() in f['name'].lower()]
            
            # Display files with view buttons
            for i, file in enumerate(filtered_files):
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.markdown(
                        f'<div class="md-file-item">📝 {file["name"]}</div>',
                        unsafe_allow_html=True
                    )
                with col2:
                    if st.button("View", key=f"view_{i}"):
                        st.session_state.selected_file = file['full_path']
                        st.rerun()
        
        if not folders and not md_files:
            st.info("No folders or markdown files found in this location.")
    
    # Preview pane
    with preview_col:
        st.markdown('<div class="preview-container">', unsafe_allow_html=True)
        st.subheader("📝 Markdown Preview")
        
        if st.session_state.selected_file:
            file_name = st.session_state.selected_file.split('/')[-1]
            st.markdown(f"**Viewing:** {file_name}")
            
            # Get and display markdown content
            content = get_file_content(bucket_name, st.session_state.selected_file)
            if content:
                try:
                    # Display markdown content
                    md_content = content.decode('utf-8')
                    st.markdown(md_content)
                    
                    # Add download link - try presigned URL first, fallback to direct URL
                    download_url = get_presigned_url(bucket_name, st.session_state.selected_file)
                    if download_url:
                        st.markdown(f"**Download:** [Click here to download]({download_url})")
                    else:
                        # Fallback to direct URL (may not work if bucket isn't public)
                        direct_url = f"https://{bucket_name}.s3.amazonaws.com/{st.session_state.selected_file}"
                        st.markdown(f"**Download:** [Click here to download]({direct_url})")
                except UnicodeDecodeError:
                    st.error("Error decoding markdown file. The file might be corrupted.")
        else:
            st.info("Select a markdown file to preview its contents")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
except Exception as e:
    st.error(f"An error occurred: {str(e)}")

# Add troubleshooting section

# Footer
st.markdown("---")
st.caption("Markdown Browser - Navigate and preview markdown files in your S3 bucket")
