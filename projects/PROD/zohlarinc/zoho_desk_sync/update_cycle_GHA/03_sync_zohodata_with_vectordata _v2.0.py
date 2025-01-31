"""
File Manipulation Details:

This script is responsible for syncing Zoho data with vector data. It follows a chronological process:
1. **List S3 Objects**: Fetches a list of files from the S3 bucket under the `<TENANT_NAME>/zohodesk-data/synced/` prefix.
2. **Identify Latest File**: Finds the latest vector data file based on datetime using regex to extract the timestamp.
3. **Load JSON from S3**: Downloads the identified latest vector data JSON and Zoho data from S3.
4. **Compare and Update Data**: Synchronizes Zoho data with vector data based on:
   - New articles.
   - Changed articles.
   - Unchanged articles.
   - Deleted/non-published articles.
   - Permission restrictions.
5. **Save Updated Data**: Uploads the updated vector data to S3 with a new filename based on the current UTC datetime.

Comparison Logic:
- **New Articles**: Articles in Zoho data but not in vector data are added directly to the updated vector data.
- **Changed Articles**: Compares `modifiedTime` between Zoho and vector data, using the more recent version.
- **Unchanged Articles**: Articles are retained if the timestamps match or the vector data is more recent.
- **Deleted/Non-Published**: Articles that are trashed or unpublished in Zoho data are excluded.
- **Permission Restrictions**: Only includes articles with permission set to 'REGISTEREDUSERS'.
"""

import json
import logging
import boto3
from datetime import datetime, timezone
import os
import re
from tqdm import tqdm
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
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# AWS S3 Configuration
s3_client = boto3.client('s3')
bucket_name = os.getenv('S3_BUCKET_NAME')
storage_prefix = f"{os.getenv('TENANT_NAME')}/zohodesk-data"

def find_latest_vectordata_file():
    """
    Finds the latest vector data file from the S3 bucket based on the timestamp in the filename.

    Returns:
        str: The S3 key of the latest vector data file.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{storage_prefix}/synced/")
        files = response.get('Contents', [])
        latest_key = None
        latest_datetime = None

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
        logger.error(f"Error finding the latest vectordata file: {e}")
        return None

def list_vectordata_files():
    """
    Lists all vector data files in the S3 bucket, sorted by timestamp.

    Returns:
        list: A list of sorted vector data file keys.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{storage_prefix}/synced/")
        files = response.get('Contents', [])
        vectordata_files = [file['Key'] for file in files if 'vectordata_' in file['Key']]

        return sorted(
            vectordata_files,
            key=lambda x: datetime.strptime(x.split('vectordata_')[1].split('.json')[0], "%Y%m%d_%H%M%S"),
            reverse=True
        )
    except Exception as e:
        logger.error(f"Error listing vector data files: {e}")
        exit(1)

def maintain_latest_files():
    """
    Keeps only the three most recent vector data files and deletes older ones from S3.
    """
    files = list_vectordata_files()
    if len(files) > 3:
        for file_to_delete in files[3:]:
            try:
                s3_client.delete_object(Bucket=bucket_name, Key=file_to_delete)
                logger.info(f"Deleted old file: {file_to_delete}")
            except Exception as e:
                logger.error(f"Error deleting file {file_to_delete}: {e}")
                exit(1)

def load_json_from_s3(key):
    """
    Loads JSON data from an S3 bucket.

    Args:
        key (str): The S3 key of the file to load.

    Returns:
        dict or list: The JSON data.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        data = json.load(response['Body'])
        logger.info(f"Data successfully loaded from {key}")
        return data
    except Exception as e:
        logger.error(f"Error loading JSON from {key}: {e}")
        return None

def save_json_to_s3(data, key):
    """
    Saves JSON data to an S3 bucket.

    Args:
        data: The data to be saved.
        key (str): The S3 key where the data will be stored.
    """
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data, indent=4))
        logger.info(f"Data successfully saved to {key}")
    except Exception as e:
        logger.error(f"Error saving JSON to {key}: {e}")
        exit(1)

def update_vectordata(zohodata, vectordata):
    """
    Synchronizes Zoho data with the vector data.

    Args:
        zohodata (list): The list of articles from Zoho.
        vectordata (list): The existing vector data.

    Returns:
        list: The updated vector data.
    """
    vectordata_dict = {article['title']: article for article in vectordata}
    updated_vectordata = []

    for article in tqdm(zohodata, desc="Syncing Zoho data with vector data"):
        metadata = article.get('metadata', {}).get('zd_metadata', {})
        latest_version_status = metadata.get('latestVersionStatus', 'Not Published')
        is_trashed = metadata.get('isTrashed', True)
        permission = metadata.get('permission', 'NONE')

        if latest_version_status == 'Published' and not is_trashed and permission == 'REGISTEREDUSERS':
            title = article.get('title', 'No Title')
            article_vector = vectordata_dict.get(title)

            modified_time_a = datetime.fromisoformat(metadata.get('modifiedTime', '1900-01-01T00:00:00.000Z').rstrip('Z'))

            if article_vector:
                vector_metadata = article_vector.get('metadata', {}).get('zd_metadata', {})
                modified_time_b = datetime.fromisoformat(vector_metadata.get('modifiedTime', '1900-01-01T00:00:00.000Z').rstrip('Z'))

                # Compare modification times
                if modified_time_a > modified_time_b:
                    updated_vectordata.append(article)
                else:
                    updated_vectordata.append(article_vector)
            else:
                # New article
                updated_vectordata.append(article)

    return updated_vectordata

def main():
    """
    Main function to synchronize Zoho data with vector data.
    """
    zohodata_key = f"{storage_prefix}/02_converted_zohodata.json"  # Zoho data key
    vectordata_key = find_latest_vectordata_file()

    # Load Zoho data
    zohodata = load_json_from_s3(zohodata_key)
    if zohodata is None:
        logger.error("Error loading Zoho data. Exiting.")
        exit(1)

    if vectordata_key is None:
        logger.warning("No vector data file found. Using only Zoho data.")
        updated_vectordata = zohodata
    else:
        vectordata = load_json_from_s3(vectordata_key)
        if vectordata is None:
            logger.error("Error loading vector data. Exiting.")
            exit(1)
        updated_vectordata = update_vectordata(zohodata, vectordata)

    # Generate new output key with UTC timestamp
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_key = f"{storage_prefix}/synced/vectordata_{timestamp}.json"

    # Save updated vector data to S3
    save_json_to_s3(updated_vectordata, output_key)

    # Maintain only the latest three vector data files
    maintain_latest_files()

if __name__ == "__main__":
    main()
