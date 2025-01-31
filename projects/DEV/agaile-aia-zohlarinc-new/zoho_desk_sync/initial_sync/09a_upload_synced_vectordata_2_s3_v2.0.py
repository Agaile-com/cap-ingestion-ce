"""
Script to upload synced vector data to an AWS S3 bucket.

This script uploads a local JSON file containing synced vector data to a specified S3 bucket.
The object name in S3 is constructed dynamically based on a timestamp if not provided.

Environment Variables:
- BASE_DIR: Base directory for all files (from .env file)
- S3_BUCKET_NAME: The name of the S3 bucket to upload the file to (from .env file)
- TENANT_NAME: The tenant name used in the S3 object path (from .env file)

Input:
- Local file to be uploaded.

Output:
- Uploads the file to the specified S3 bucket.
"""

import boto3
import os
from datetime import datetime, timezone
from botocore.exceptions import NoCredentialsError
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

# Load necessary variables
BASE_DIR = os.getenv('BASE_DIR')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
TENANT_NAME = os.getenv('TENANT_NAME')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def upload_file_to_s3(file_path, bucket, object_name=None):
    """
    Uploads a file to an S3 bucket.
    
    :param file_path: Path to the file on the local system
    :param bucket: Name of the S3 bucket
    :param object_name: S3 object name. If not specified, a timestamped name is generated.
    :return: True if the upload was successful, otherwise False
    """
    if object_name is None:
        # Generate a timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        # Use TENANT_NAME in the object path
        object_name = f"{TENANT_NAME}/zohodesk-data/synced/vectordata_{timestamp}.json"

    # Create an S3 client
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket, object_name)
        logging.info(f"File {file_path} successfully uploaded to {bucket}/{object_name}.")
        return True
    except FileNotFoundError:
        logging.error(f"File {file_path} not found.")
        return False
    except NoCredentialsError:
        logging.error("AWS credentials not available.")
        return False
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return False

def main():
    # Dynamically construct the file path using BASE_DIR
    file_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/07_converted_zohodata.json')

    # Upload the file to the S3 bucket
    success = upload_file_to_s3(file_path, S3_BUCKET_NAME)
    if success:
        logging.info("File upload completed successfully.")
    else:
        logging.error("File upload failed.")

if __name__ == "__main__":
    main()