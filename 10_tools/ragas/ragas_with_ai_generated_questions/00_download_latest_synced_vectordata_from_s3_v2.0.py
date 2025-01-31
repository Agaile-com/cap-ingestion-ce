"""
Script to download the latest synced vector data from an AWS S3 bucket.

This script downloads the most recent JSON file containing synced vector data from a specified S3 bucket.
The script identifies the latest file based on the timestamp in the filename.

Environment Variables:
- BASE_DIR: Base directory for all files (from .env file)
- S3_BUCKET_NAME: The name of the S3 bucket to download from (from .env file)
- TENANT_NAME: The tenant name used in the S3 object path (from .env file)

Output:
- Downloads the latest vector data file to the local system
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
parent_dir = script_dir.parent.parent
sys.path.append(str(parent_dir))

from utils import load_env
load_env()

# Load necessary variables
BASE_DIR = os.getenv('BASE_DIR')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
TENANT_NAME = os.getenv('TENANT_NAME')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_latest_vectordata_file(bucket):
    """
    Gets the latest vector data file from S3 based on timestamp in filename.
    
    :param bucket: Name of the S3 bucket
    :return: The key (path) of the latest file, or None if no files found
    """
    s3_client = boto3.client('s3')
    prefix = f"{TENANT_NAME}/zohodesk-data/synced/"
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            logging.error(f"No files found in {bucket}/{prefix}")
            return None

        # Filter for vectordata files and sort by timestamp
        vector_files = [
            obj['Key'] for obj in response['Contents']
            if 'vectordata_' in obj['Key'] and obj['Key'].endswith('.json')
        ]
        
        if not vector_files:
            logging.error("No vector data files found")
            return None

        # Sort by timestamp in filename (assuming format vectordata_YYYYMMDD_HHMMSS.json)
        latest_file = sorted(vector_files, reverse=True)[0]
        return latest_file

    except Exception as e:
        logging.error(f"Error listing objects in S3: {e}")
        return None

def download_file_from_s3(bucket, object_name, file_path):
    """
    Downloads a file from an S3 bucket.
    
    :param bucket: Name of the S3 bucket
    :param object_name: S3 object key to download
    :param file_path: Local path to save the file
    :return: True if download was successful, otherwise False
    """
    s3_client = boto3.client('s3')
    try:
        s3_client.download_file(bucket, object_name, file_path)
        logging.info(f"File successfully downloaded from {bucket}/{object_name} to {file_path}")
        return True
    except NoCredentialsError:
        logging.error("AWS credentials not available")
        return False
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return False

def main():
    # Get the latest vector data file from S3
    latest_file = get_latest_vectordata_file(S3_BUCKET_NAME)
    if not latest_file:
        logging.error("No file to download")
        return False

    # Get the directory where the script is located and create data directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, 'data')
    os.makedirs(data_dir, exist_ok=True)

    # Set the local file path with the new filename in the data directory
    local_file_path = os.path.join(data_dir, 'latest_vectordata_zoho_sync.json')

    # Download the file
    success = download_file_from_s3(S3_BUCKET_NAME, latest_file, local_file_path)
    if success:
        logging.info("File download completed successfully")
    else:
        logging.error("File download failed")

if __name__ == "__main__":
    main()