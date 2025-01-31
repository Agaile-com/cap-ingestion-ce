"""
Script to fetch and save Zoho Desk articles' metadata.

This script fetches metadata for all articles from Zoho Desk filtered by departmentId and categoryId,
and saves them to a JSON file.

Environment Variables:
- BASE_DIR: Base directory for all files (from .env file)
- CLIENT_ID: Zoho OAuth client ID (from .env file)
- CLIENT_SECRET: Zoho OAuth client secret (from .env file)
- REDIRECT_URI: Zoho OAuth redirect URI (from .env file)
- REFRESH_TOKEN: Zoho OAuth refresh token (from .env file)
- TOKEN_URL: Zoho OAuth token URL (from .env file)
- ARTICLES_URL: Zoho Desk articles API URL (from .env file)

Input:
- Fetches articles' metadata from Zoho Desk.

Output:
- Saves articles' metadata to a JSON file.
"""

import sys
from pathlib import Path

# Dynamically add the parent directory (zoho_desk_sync) to sys.path
script_dir = Path(__file__).resolve().parent
parent_dir = script_dir.parent  # This will resolve to zoho_desk_sync
sys.path.append(str(parent_dir))

# Now import utils after sys.path is set
from utils import load_env

import logging
import requests
import json
import os
from tqdm import tqdm

# Load environment variables using the helper function
load_env()

# Check if BASE_DIR is loaded correctly
BASE_DIR = os.getenv('BASE_DIR')
if BASE_DIR is None:
    raise ValueError("BASE_DIR is not set. Check if it's properly defined in the .env file.")
else:
    print(f"BASE_DIR loaded: {BASE_DIR}")

# Set the output file path dynamically using BASE_DIR
output_file_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/04_zohodata_metadata.json')
print(f"Output file path: {output_file_path}")

# Load necessary variables
BASE_DIR = os.getenv('BASE_DIR')
DEPARTMENT_ID = os.getenv('DEPARTMENT_ID')
CATEGORY_ID = os.getenv('CATEGORY_ID')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')

# Zoho API URLs loaded from environment variables
token_url = os.getenv('TOKEN_URL')
articles_url = os.getenv('ARTICLES_URL')

# Set the output file path dynamically using BASE_DIR
output_file_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/04_zohodata_metadata.json')

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Prepare the data for the OAuth token request
data = {
    'grant_type': 'refresh_token',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'redirect_uri': REDIRECT_URI,
    'refresh_token': REFRESH_TOKEN
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

# Function to fetch and save all articles' metadata to a JSON file
def save_all_articles_to_json(access_token, output_path, department_id, category_id):
    collected_articles = []
    headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
    params = {'limit': 50}
    page = 1
    has_more_pages = True

    while has_more_pages:
        logger.info(f"Fetching articles, page {page}")
        response = requests.get(articles_url, headers=headers, params=params)
        response.raise_for_status()
        articles_data = response.json()

        articles = articles_data.get('data', [])
        for article in articles:
            if article.get('departmentId') == department_id and article.get('categoryId') == category_id:
                # Fetch detailed article data dynamically
                detail_response = requests.get(f"{articles_url}/{article['id']}", headers=headers)
                detail_response.raise_for_status()
                detailed_article_data = detail_response.json()
                collected_articles.append(detailed_article_data)

        # Pagination logic
        if articles:
            page += 1
            params['from'] = page * params['limit']
        else:
            has_more_pages = False

    # Save the collected articles to a JSON file
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(collected_articles, file, indent=4, ensure_ascii=False)
    logger.info(f"Articles successfully saved. Number of articles: {len(collected_articles)}")

# Main execution
try:
    save_all_articles_to_json(access_token, output_file_path, DEPARTMENT_ID, CATEGORY_ID)
except Exception as e:
    logger.exception(f"Error saving articles: {e}")