import logging
import requests
import json
import os
from pathlib import Path
import sys

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
logger = logging.getLogger()

# Environment variables
BASE_DIR = os.getenv('BASE_DIR')
TOKEN_URL = os.getenv('TOKEN_URL')
ARTICLES_URL = os.getenv('ARTICLES_URL')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
ORG_ID = os.getenv('ORG_ID')
DEPARTMENT_ID = os.getenv('DEPARTMENT_ID')
CATEGORY_ID = os.getenv('CATEGORY_ID')
PERMISSIONS = os.getenv('PERMISSIONS')

# File path to read articles
input_file_path = '/Users/joachimkohl/dev/ingestion-projects/projects/DEV/agaile-aia-zohlarinc-new/zoho_desk_sync/update_cycle_GHA/data/zz01_ZD_relevant_ids_and_variables.json'

# Data for the POST request to fetch OAuth token
data = {
    'grant_type': 'refresh_token',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'redirect_uri': REDIRECT_URI,
    'refresh_token': REFRESH_TOKEN
}

def get_access_token():
    try:
        logger.info("Requesting access token from Zoho OAuth API...")
        token_response = requests.post(TOKEN_URL, headers={'Content-Type': 'application/x-www-form-urlencoded'}, data=data)
        token_response.raise_for_status()
        access_token = token_response.json().get('access_token')
        logger.info("Successfully obtained access token.")
        return access_token
    except requests.RequestException as e:
        logger.error(f"Failed to request access token: {e}")
        raise

def read_articles_from_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            articles = json.load(f)
        logger.info(f"Successfully read articles from {file_path}")
        return articles
    except IOError as e:
        logger.error(f"Failed to read articles from file: {e}")
        raise

def create_article_in_zoho(access_token, article):
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json',
        'orgId': ORG_ID
    }
    # Prepare the data for creating an article
    data = {
        'title': article.get('title'),
        'answer': article.get('answer'),
        'status': article.get('status', 'Published'),  # Default to 'Draft' if not specified
        'categoryId': article['category']['id'],
        'departmentId': DEPARTMENT_ID,
        'permission': PERMISSIONS  # Use the permission from the .env file
    }
    try:
        response = requests.post(ARTICLES_URL, headers=headers, json=data)
        response.raise_for_status()
        logger.info(f"Article '{article.get('title')}' created successfully.")
    except requests.RequestException as e:
        logger.error(f"Error creating article '{article.get('title')}': {e}")
        logger.error(f"Response content: {response.content}")
        raise

def create_articles_in_zoho(access_token, articles):
    for article in articles:
        create_article_in_zoho(access_token, article)

# Main logic
if __name__ == "__main__":
    try:
        access_token = get_access_token()
        articles = read_articles_from_json(input_file_path)
        create_articles_in_zoho(access_token, articles)
    except Exception as e:
        logger.exception("Error occurred during the article creation process: %s", e)