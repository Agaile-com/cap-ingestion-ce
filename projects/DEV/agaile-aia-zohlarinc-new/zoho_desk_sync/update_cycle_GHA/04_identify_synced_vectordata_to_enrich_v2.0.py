"""
File Manipulation Details:

This script identifies and separates the latest vector data file based on the presence of keywords and combined text. It follows these steps:

1. **Locate Latest Vector Data File**:
   - Fetches the most recent file from S3 under the prefix `<TENANT_NAME>/zohodesk-data/synced/`.
   - Filename format: `03_synced_vectordata_YYYY-MM-DD_HH-MM-SS.json`.

2. **Load JSON Data from S3**:
   - Loads the latest vector data file into memory as a JSON object.

3. **Delete Existing Files**:
   - Deletes any existing files in `<TENANT_NAME>/zohodesk-data/enriched/`, including:
     - `01_synced_vectordata_with_both.json`
     - `02_synced_vectordata_with_keywords_only.json`
     - `03_synced_vectordata_with_combined_only.json`
     - `04_synced_vectordata_without_both.json`.

4. **Separate Data**:
   - The data is separated into four categories based on the presence of keywords and combined text.

5. **Save Separated Data to S3**:
   - Saves the separated data into specific files within the S3 `<TENANT_NAME>/zohodesk-data/enriched/` directory.
"""

import json
import logging
import boto3
import re
from datetime import datetime
from botocore.exceptions import ClientError

import os
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

# AWS S3 Configuration
s3_client = boto3.client('s3')
bucket_name = os.getenv('S3_BUCKET_NAME')
storage_prefix = f"{os.getenv('TENANT_NAME')}/zohodesk-data"

def find_latest_vectordata_file():
    """
    Finds the most recent vector data file based on the timestamp in the filename.

    Returns:
        str: The S3 key of the latest vector data file.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{storage_prefix}/synced/")
        files = response.get('Contents', [])
        latest_datetime = None
        latest_key = None

        # Regex to match files like '<TENANT_NAME>/zohodesk-data/synced/vectordata_YYYYMMDD_HHMMSS.json'
        for file in files:
            key = file['Key']
            match = re.match(rf'^{re.escape(storage_prefix)}/synced/vectordata_(\d{{8}}_\d{{6}}).json$', key)
            if match:
                file_datetime = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
                if latest_datetime is None or file_datetime > latest_datetime:
                    latest_datetime = file_datetime
                    latest_key = key

        return latest_key
    except Exception as e:
        logging.error(f"Error finding the latest vector data file: {e}")
        return None

def load_json_from_s3(key):
    """
    Loads JSON data from an S3 bucket.

    Args:
        key (str): The S3 key to load.

    Returns:
        dict: The loaded JSON data.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        data = json.load(response['Body'])
        logging.info(f"Data successfully loaded from {key}")
        return data
    except ClientError as e:
        logging.error(f"Error loading JSON from {key}: {e}")
        return None

def delete_existing_files(prefix):
    """
    Deletes files that match a specific pattern in the S3 bucket.

    Args:
        prefix (str): The S3 prefix to search for files to delete.
    """
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        for page in pages:
            for obj in page.get('Contents', []):
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                logging.info(f"Deleted existing file: {obj['Key']}")
    except ClientError as e:
        logging.error(f"Error deleting files: {e}")
        exit(1)

def save_json_to_s3(data, key):
    """
    Saves JSON data to an S3 bucket.

    Args:
        data (dict): The JSON data to save.
        key (str): The S3 key to save the data under.
    """
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data, indent=4))
        logging.info(f"Data saved to S3: {key}")
    except ClientError as e:
        logging.error(f"Error saving JSON to S3: {key}: {e}")
        exit(1)

def separate_data(data):
    """
    Separates the data into four categories:
    - with both keywords and combined_text
    - with keywords only
    - with combined_text only
    - without both

    Args:
        data (list): The loaded vector data.

    Returns:
        tuple: Four lists of separated data.
    """
    with_both = []
    with_keywords_only = []
    with_combined_only = []
    without_both = []

    for item in data:
        # Handle 'keywords' as a list of strings
        keywords = item.get('keywords', [])
        if isinstance(keywords, str):
            keywords = keywords.strip().split()

        combined_text = item.get('combined_text', '').strip()
        has_keywords = bool(keywords)
        has_combined_text = bool(combined_text)

        if has_keywords and has_combined_text:
            with_both.append(item)
        elif has_keywords:
            with_keywords_only.append(item)
        elif has_combined_text:
            with_combined_only.append(item)
        else:
            without_both.append(item)

    return with_both, with_keywords_only, with_combined_only, without_both

def main():
    """
    Main function to find the latest vector data, separate it into categories, and save the results back to S3.
    """
    enriched_prefix = f"{storage_prefix}/enriched/"
    
    # Locate the latest vector data file
    latest_key = find_latest_vectordata_file()
    if latest_key is None:
        logging.error("No vector data file found. Exiting.")
        exit(1)

    # Load the original data from S3
    original_data = load_json_from_s3(latest_key)
    if original_data is None:
        logging.error("Failed to load original data. Exiting.")
        exit(1)

    # Separate the data into four categories
    data_with_both, data_with_keywords_only, data_with_combined_only, data_without_both = separate_data(original_data)

    # Delete existing files before saving new data
    delete_existing_files(f"{enriched_prefix}01_synced_vectordata_with_both.json")
    delete_existing_files(f"{enriched_prefix}02_synced_vectordata_with_keywords_only.json")
    delete_existing_files(f"{enriched_prefix}03_synced_vectordata_with_combined_only.json")
    delete_existing_files(f"{enriched_prefix}04_synced_vectordata_without_both.json")

    # Save the separated data to S3
    save_json_to_s3(data_with_both, f"{enriched_prefix}01_synced_vectordata_with_both.json")
    save_json_to_s3(data_with_keywords_only, f"{enriched_prefix}02_synced_vectordata_with_keywords_only.json")
    save_json_to_s3(data_with_combined_only, f"{enriched_prefix}03_synced_vectordata_with_combined_only.json")
    save_json_to_s3(data_without_both, f"{enriched_prefix}04_synced_vectordata_without_both.json")

    logging.info("Data successfully separated and saved to S3.")

if __name__ == "__main__":
    main()
