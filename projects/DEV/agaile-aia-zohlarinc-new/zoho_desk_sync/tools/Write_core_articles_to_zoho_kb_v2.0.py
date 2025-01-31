"""
Zoho Desk Article Synchronization Script

This script performs the following operations:
1. Fetches an access token from Zoho OAuth API using a refresh token.
2. Loads articles from a local JSON file.
3. Sends the articles to Zoho Desk API, updating them as needed.

Environment Variables:
- BASE_DIR: The base directory for the project files.
- DEPARTMENT_ID: The department ID for Zoho articles.
- CATEGORY_ID: The category ID for Zoho articles.
- CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, REFRESH_TOKEN: Zoho OAuth credentials.
- ORG_ID: The organization ID for the Zoho Desk API.
- ARTICLES_URL: The base URL for Zoho Desk articles.
- TOKEN_URL: The URL to retrieve the access token.

Dependencies:
- `dotenv` for loading environment variables.
- `requests` for handling HTTP requests.

"""

import logging
import requests
import json
import os
from utils import load_env

# Load environment variables using the helper function
load_env()
# Setup logging configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()



# API and authentication details from the .env file
DEPARTMENT_ID = os.getenv('DEPARTMENT_ID')
CATEGORY_ID = os.getenv('CATEGORY_ID')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
ORG_ID = os.getenv('ORG_ID')  # Your Zoho organization ID
ARTICLES_BASE_URL = os.getenv('ARTICLES_URL')  # Base URL from .env
TOKEN_URL = os.getenv('TOKEN_URL')  # Token URL from .env

# Construct the complete articles URL
articles_url = f'{ARTICLES_BASE_URL}?orgId={ORG_ID}'

def get_access_token():
    """
    Retrieves the access token from Zoho OAuth API using the refresh token.

    Returns:
        str: The access token required for Zoho API requests.
    """
    logger.debug('Fetching access token with refresh token')
    
    payload = {
        'refresh_token': REFRESH_TOKEN,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'refresh_token'
    }

    try:
        response = requests.post(TOKEN_URL, data=payload)
        response.raise_for_status()
        
        access_token = response.json().get('access_token')
        logger.debug(f"Access token received: {access_token}")
        return access_token
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching access token: {http_err}")
    except Exception as err:
        logger.error(f"Error occurred while fetching access token: {err}")
    
    return None

def send_article(article, access_token):
    """
    Sends a single article to Zoho Desk API.

    Args:
        article (dict): The article data to be sent.
        access_token (str): The OAuth token for Zoho API authentication.
    """
    logger.debug(f"Sending article '{article.get('title', 'Untitled Article')}' to Zoho Desk")
    
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        'categoryId': CATEGORY_ID,
        'title': article.get('title', 'Untitled Article'),
        'answer': article.get('answer', 'No answer available'),
        'status': article.get('status', 'Published'),
        'permission': article.get('permission', 'REGISTEREDUSERS')
    }

    try:
        logger.debug(f"Payload for article '{article['title']}': {json.dumps(payload, indent=2)}")
        response = requests.post(articles_url, headers=headers, json=payload)
        response.raise_for_status()
        logger.info(f"Article '{article['title']}' added successfully.")
        logger.debug(f"Response body: {response.text}")
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error while sending article '{article['title']}': {http_err}")
        logger.error(f"Response body: {response.text}")
    except Exception as err:
        logger.error(f"Error while sending article '{article['title']}': {err}")

def sync_articles_from_json():
    """
    Synchronizes articles from a local JSON file to Zoho Desk.

    Loads articles from the specified JSON file and sends them to Zoho Desk API.
    """
    logger.debug("Starting sync_articles_from_json")
    
    # Fetch the access token
    access_token = get_access_token()
    if not access_token:
        logger.error("Failed to retrieve access token. Exiting the sync process.")
        return

    # Define the path to the JSON file
    json_file_path = os.path.join(os.getenv('BASE_DIR'), 'AGAILE-ZOHO-SYNC-DEV/zoho_desk_sync/initial_sync/data/zohlar-core_English_translated.json')
    logger.debug(f"Loading articles from JSON file: {json_file_path}")
    
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            articles_data = json.load(file)
            logger.debug(f"{len(articles_data)} articles loaded from JSON file")
    except Exception as e:
        logger.error(f"Error reading JSON file: {e}")
        return

    # Send articles to Zoho Desk API, log progress after each article
    logger.debug("Sending articles to Zoho Desk API")
    for idx, article in enumerate(articles_data, start=1):
        send_article(article, access_token)
        logger.info(f"Progress: {idx}/{len(articles_data)} articles sent.")

if __name__ == '__main__':
    """
    Main entry point for synchronizing articles from a JSON file to Zoho Desk API.
    """
    logger.info("Starting article synchronization")
    sync_articles_from_json()
    logger.info("Article synchronization completed")
