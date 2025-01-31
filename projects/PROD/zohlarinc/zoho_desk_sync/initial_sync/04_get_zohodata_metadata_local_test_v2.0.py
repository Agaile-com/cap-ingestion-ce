"""
Local test script to fetch a specific Zoho Desk article's metadata.

This script fetches metadata for a specific article from Zoho Desk and saves it to a JSON file for testing purposes.

Environment Variables:
- BASE_DIR: Base directory for all files (from .env file)
- CLIENT_ID: Zoho OAuth client ID (from .env file)
- CLIENT_SECRET: Zoho OAuth client secret (from .env file)
- REDIRECT_URI: Zoho OAuth redirect URI (from .env file)
- REFRESH_TOKEN: Zoho OAuth refresh token (from .env file)
- TOKEN_URL: Zoho OAuth token URL (from .env file)
- ARTICLES_URL: Zoho Desk articles API URL (from .env file)

Input:
- Fetches a specific article's metadata from Zoho Desk.

Output:
- Saves the article's metadata to a JSON file.
"""

import logging
import requests
import json
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

# Load necessary variables
BASE_DIR = os.getenv('BASE_DIR')
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
redirect_uri = os.getenv('REDIRECT_URI')
refresh_token = os.getenv('REFRESH_TOKEN')

# Zoho API URLs loaded from environment variables
token_url = os.getenv('TOKEN_URL')
articles_url = os.getenv('ARTICLES_URL')  # Base URL for fetching articles

# Set the output file path dynamically using BASE_DIR
output_file_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/04_zohodata_metadata_local_test.json')

# Logging configuration
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

# Prepare the data for the OAuth token request
data = {
    'grant_type': 'refresh_token',
    'client_id': client_id,
    'client_secret': client_secret,
    'redirect_uri': redirect_uri,
    'refresh_token': refresh_token
}

# Request Zoho OAuth token
try:
    logger.info("Requesting access token from Zoho OAuth API...")
    token_response = requests.post(token_url, headers={'Content-Type': 'application/x-www-form-urlencoded'}, data=data)
    token_response.raise_for_status()
    access_token = token_response.json().get('access_token')
    logger.info("Access token received successfully.")
except Exception as e:
    logger.error(f"Error requesting OAuth token: {e}")
    raise

# Specific article ID for local test
article_id = '171790000000440178'  # Replace with the actual article ID you want to fetch

# Function to fetch and save a specific article's metadata to a JSON file
def fetch_and_save_article(access_token, article_id, output_path):
    headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
    
    # Fetch the specific article using the dynamic URL
    response = requests.get(f"{articles_url}/{article_id}", headers=headers)
    response.raise_for_status()
    
    # Save the article data to the output JSON file
    article_data = response.json()
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(article_data, file, indent=4, ensure_ascii=False)
    logger.info("Article successfully saved.")

# Main execution
try:
    fetch_and_save_article(access_token, article_id, output_file_path)
except Exception as e:
    logger.exception(f"Error fetching article: {e}")
