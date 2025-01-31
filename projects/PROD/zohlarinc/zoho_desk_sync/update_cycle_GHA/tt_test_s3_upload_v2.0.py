"""
S3 File Upload Script

This script handles uploading a file from a local path to an AWS S3 bucket.

File Paths:
- BASE_DIR: The base directory is configured using an environment variable (`BASE_DIR`).
- The file path is defined relative to the BASE_DIR, ensuring flexibility and easier adaptability across environments.

Process Overview:
1. **File Path**: The file to be uploaded is defined relative to the BASE_DIR.
2. **S3 Upload**: The file is uploaded to the specified S3 bucket using the boto3 library.
3. **Logging**: Logs are generated to indicate success or failure during the upload process.

Environment Variables:
- `BASE_DIR`: The base directory for the local file.
- `S3_BUCKET_NAME`: The AWS S3 bucket name.

Error Handling:
- Handles errors such as missing file, AWS credentials, and general exceptions during upload.
"""

import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError
import logging
import sys
from pathlib import Path

# Dynamically add the parent directory (zoho_desk_sync) to sys.path
script_dir = Path(__file__).resolve().parent
parent_dir = script_dir.parent  # This will resolve to zoho_desk_sync
sys.path.append(str(parent_dir))

# Now import utils after sys.path is set
from utils import load_env
# Load environment variables using the helper function
load_env()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def upload_file_to_s3(file_path, bucket, object_name=None):
    """
    Uploads a local file to an S3 bucket.

    Args:
        file_path (str): Path to the local file to upload.
        bucket (str): Name of the S3 bucket.
        object_name (str, optional): S3 object name. If not specified, the file name is used.

    Returns:
        bool: True if the file was uploaded successfully, False otherwise.
    """
    if object_name is None:
        object_name = os.path.basename(file_path)

    # Create S3 client
    s3_client = boto3.client('s3')

    try:
        # Upload file to S3
        s3_client.upload_file(file_path, bucket, object_name)
        logging.info(f"File '{file_path}' successfully uploaded to '{bucket}/{object_name}'.")
        return True
    except FileNotFoundError:
        logging.error(f"File '{file_path}' not found.")
        return False
    except NoCredentialsError:
        logging.error("AWS credentials not available.")
        return False
    except ClientError as e:
        logging.error(f"Client error during upload: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return False

def main():
    """
    Main function to set the file path and bucket name, then call the upload function.

    Environment Variables:
        BASE_DIR: The base directory for project files.
        S3_BUCKET_NAME: The name of the AWS S3 bucket.
    """
    # Get the BASE_DIR from environment variables and construct the relative file path
    base_dir = os.getenv('BASE_DIR', '/your/default/base/dir/')
    if not base_dir:
        logging.error("BASE_DIR environment variable is not set.")
        return

    # Set the relative path from BASE_DIR and the actual file name (to be dynamically changed)
    relative_path = 'path/to/your/file'  # Placeholder for your path inside the project
    file_name = 'your_file_name.json'  # Placeholder for the actual file name
    file_path = os.path.join(base_dir, relative_path, file_name)

    # Get the S3 bucket name from environment variables
    bucket_name = os.getenv('S3_BUCKET_NAME', 'your-default-bucket')

    # Ensure the bucket name is retrieved from the environment
    if not bucket_name:
        logging.error("S3_BUCKET_NAME environment variable is not set.")
        return

    # Upload the file to S3
    if not upload_file_to_s3(file_path, bucket_name):
        logging.error("File upload failed.")
    else:
        logging.info("File upload completed successfully.")

if __name__ == "__main__":
    main()
