"""
Script to sync Zoho Desk article data with the vector database.

This script compares Zoho Desk article data with the existing vector database entries and updates
the vector data based on the latest modified times of the articles. The updated vector data is saved
to a new JSON file.

Environment Variables:
- BASE_DIR: Base directory for all files (from .env file)
- PERMISSIONS: The required permission level to sync articles (from .env file)

Input:
- Reads JSON files containing Zoho Desk articles and vector data.

Output:
- Saves the updated vector database to a new JSON file.
"""

import json
from datetime import datetime
import logging
from tqdm import tqdm
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

# Load base directory and permissions from environment variables
BASE_DIR = os.getenv('BASE_DIR')
PERMISSIONS = os.getenv('PERMISSIONS', 'REGISTEREDUSERS')

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def load_json_data(filename):
    """Loads JSON data from a file."""
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            logging.info(f"Loading data from {filename}")
            return json.load(file)
    except Exception as e:
        logging.error(f"Error loading {filename}: {e}")
        return None

def save_json_data(data, filename):
    """Saves JSON data to a file."""
    try:
        with open(filename, 'w', encoding='utf-8') as file:
            logging.info(f"Saving data to {filename}")
            json.dump(data, file, indent=4, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Error saving to {filename}: {e}")

def parse_isoformat(date_str):
    """Parses an ISO format string to a datetime object. Returns datetime.min on error."""
    try:
        if date_str:
            return datetime.fromisoformat(date_str.rstrip('Z'))
        else:
            return datetime.min
    except ValueError as e:
        logging.error(f"Error parsing date string: {e}")
        return datetime.min

def update_vectordata(zohodata, vectordata):
    """Updates the vector data by comparing with Zoho Desk article data."""
    vectordata_dict = {article['title']: article for article in vectordata}
    updated_vectordata = []

    for article in tqdm(zohodata, desc="Comparing Zoho data with vector data"):
        # Safely access nested fields with default values
        latestVersionStatus = article.get('metadata', {}).get('zd_metadata', {}).get('latestVersionStatus', 'Not Published')
        isTrashed = article.get('metadata', {}).get('zd_metadata', {}).get('isTrashed', True)
        permission = article.get('metadata', {}).get('zd_metadata', {}).get('permission', 'NONE')

        if latestVersionStatus == 'Published' and not isTrashed and permission == PERMISSIONS:
            title = article.get('title', 'No Title')
            article_vector = vectordata_dict.get(title)
            modified_time_a = parse_isoformat(article.get('metadata', {}).get('zd_metadata', {}).get('modifiedTime', '1900-01-01T00:00:00.000Z'))

            if article_vector:
                # Compare modified times
                modified_time_b = parse_isoformat(article_vector.get('metadata', {}).get('zd_metadata', {}).get('modifiedTime', '1900-01-01T00:00:00.000Z'))

                if modified_time_a > modified_time_b:
                    updated_vectordata.append(article)  # Zoho article is newer
                else:
                    updated_vectordata.append(article_vector)  # Vector article is newer or same
            else:
                updated_vectordata.append(article)  # Add if not in vector data

    return updated_vectordata

def main():
    # Dynamically construct paths using BASE_DIR
    zohodata_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/07_converted_zohodata.json')
    vectordata_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/05_matched_vectordata.json')
    output_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/08_synced_vectordata.json')

    # Load Zoho data and vector data
    zohodata = load_json_data(zohodata_path)
    vectordata = load_json_data(vectordata_path)

    if zohodata is None or vectordata is None:
        logging.error("Error loading data. Aborting.")
        return

    # Update vector data by comparing with Zoho data
    updated_vectordata = update_vectordata(zohodata, vectordata)

    # Save the updated vector data
    save_json_data(updated_vectordata, output_path)
    logging.info(f"Updated vector data saved to {output_path}")

if __name__ == "__main__":
    main()
