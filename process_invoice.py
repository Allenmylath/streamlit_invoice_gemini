import os
import google.generativeai as genai
from PIL import Image
import io

def read_file(file_bytes, file_type):
    """
    Read and prepare file content for processing
    
    Args:
        file_bytes: The raw bytes of the uploaded file
        file_type: The MIME type of the file
        
    Returns:
        dict: A dictionary containing the processed content and MIME type
    """
    # Only handle image files
    if file_type.startswith('image/'):
        image = Image.open(io.BytesIO(file_bytes))
        return {"image": image, "mime_type": file_type}
    else:
        return None

def process_invoice(api_key, file_content, file_type):
    """
    Process an invoice using Gemini API
    
    Args:
        api_key: The Gemini API key
        file_content: The raw bytes of the uploaded file
        file_type: The MIME type of the file
        
    Returns:
        str: The extracted markdown information
    """
    # Configure the Gemini API with API key
    genai.configure(api_key=api_key)
    
    # Get file content
    content = read_file(file_content, file_type)
    if not content:
        return "Error: Unsupported file format. Only JPG and PNG files are supported."
    
    # Set up the model - we're only using images
    model = genai.GenerativeModel('gemini-1.5-flash')
    
    # Define the prompt for invoice extraction
    prompt = """
    Extract info from the invoice :
    
    
    Make sure to return only valid markdown.
    """
    
    # Generate content - only handling images
    try:
        response = model.generate_content([prompt, content["image"]])
        
        # Extract the text from the response
        markdown_str = response.text.strip()
        return markdown_str
    except Exception as e:
        return f"Error during processing: {str(e)}"
