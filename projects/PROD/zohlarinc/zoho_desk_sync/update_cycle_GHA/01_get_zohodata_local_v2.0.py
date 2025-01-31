import logging
import requests
import json
import os

from bs4 import BeautifulSoup
import unicodedata
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
logger = logging.getLogger()

# Environment variables
BASE_DIR = os.getenv('BASE_DIR')  # Use base directory from .env
local_output_path = os.path.join(BASE_DIR, 'zoho_desk_sync/update_cycle_GHA/data/zz01_ZD_relevant_ids_and_variables.json')

# API endpoints and authentication details from environment variables
token_url = os.getenv('TOKEN_URL')  # Token URL from .env
articles_url = os.getenv('ARTICLES_URL')  # Articles API URL from .env
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
redirect_uri = os.getenv('REDIRECT_URI')
refresh_token = os.getenv('REFRESH_TOKEN')

# Zoho Department and Category IDs
specific_department_id = os.getenv('DEPARTMENT_ID')
specific_category_id = os.getenv('CATEGORY_ID')

# Data for the POST request to fetch OAuth token
data = {
    'grant_type': 'refresh_token',
    'client_id': client_id,
    'client_secret': client_secret,
    'redirect_uri': redirect_uri,
    'refresh_token': refresh_token
}

def get_access_token():
    """
    Fetches an access token from the Zoho OAuth API.

    Sends a POST request to Zoho's OAuth endpoint to retrieve an access token
    using the refresh token.

    Returns:
        str: The access token for Zoho API.
    
    Raises:
        requests.RequestException: If the request to the OAuth API fails.
    """
    try:
        logger.info("Requesting access token from Zoho OAuth API...")
        token_response = requests.post(token_url, headers={'Content-Type': 'application/x-www-form-urlencoded'}, data=data)
        token_response.raise_for_status()
        access_token = token_response.json().get('access_token')
        logger.info("Successfully obtained access token.")
        return access_token
    except requests.RequestException as e:
        logger.error(f"Failed to request access token: {e}")
        raise

def html_to_plain_text(html_content):
    """
    Converts HTML content to plain text.

    Args:
        html_content (str): The HTML content to be converted.

    Returns:
        str: Plain text extracted from the HTML content.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    return soup.get_text(separator=' ')

def normalize_unicode_characters(data):
    """
    Recursively normalizes all string characters in a nested JSON object using Unicode NFKC normalization.

    Args:
        data: The data structure (str, list, dict) to be normalized.

    Returns:
        The normalized data structure with all string characters normalized using Unicode NFKC.
    """
    if isinstance(data, str):
        return unicodedata.normalize('NFKC', data)
    elif isinstance(data, list):
        return [normalize_unicode_characters(item) for item in data]
    elif isinstance(data, dict):
        return {key: normalize_unicode_characters(value) for key, value in data.items()}
    else:
        return data

def save_all_articles_to_json(access_token, department_id, category_id):
    """
    Fetches all published articles from Zoho Desk and saves them as a JSON file locally.

    This function retrieves all articles from the Zoho Desk API filtered by department ID and category ID.
    It handles pagination and processes the articles in batches. The content of the articles is normalized
    (including HTML conversion to plain text), and the final output is saved as a local JSON file.

    Args:
        access_token (str): The access token for Zoho API.
        department_id (str): The department ID to filter articles by.
        category_id (str): The category ID to filter articles by.

    Raises:
        requests.RequestException: If any request to the Zoho Desk API fails.
    """
    collected_articles = []
    headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
    params = {
        'limit': 50,
        'sortBy': 'createdTime',
        'status': 'Published',
        'categoryId': category_id
    }
    page = 1
    has_more_pages = True

    while has_more_pages:
        params['from'] = (page - 1) * params['limit'] + 1
        logger.info(f"Fetching articles, starting from {params['from']}")
        response = requests.get(articles_url, headers=headers, params=params)

        if response.status_code == 422:
            logger.error(f"Received 422 error: {response.text}")
            break

        response.raise_for_status()
        articles_data = response.json()
        articles = articles_data.get('data', [])
        logger.debug(f"Fetched {len(articles)} articles")

        if not articles:
            has_more_pages = False
        else:
            page += 1

        # Fetch each article in detail
        for article in articles:
            if article.get('departmentId') == department_id:
                logger.debug(f"Processing article ID: {article['id']}")
                detail_response = requests.get(f"{articles_url}/{article['id']}", headers=headers)
                detail_response.raise_for_status()
                detailed_article_data = detail_response.json()

                # Convert HTML content to plain text
                if 'answer' in detailed_article_data:
                    detailed_article_data['answer'] = html_to_plain_text(detailed_article_data['answer'])

                # Normalize the data and append to collected articles
                detailed_article_data = normalize_unicode_characters(detailed_article_data)
                collected_articles.append(detailed_article_data)

    # Normalize the entire collected articles list
    collected_articles = normalize_unicode_characters(collected_articles)

    # Save the data to a local JSON file
    with open(local_output_path, 'w', encoding='utf-8') as f:
        json.dump(collected_articles, f, ensure_ascii=False, indent=4)
    logger.info(f"Articles successfully saved to local file: {local_output_path}")

# Main logic
if __name__ == "__main__":
    """
    Main entry point of the script.

    This section fetches the access token, retrieves all relevant articles from Zoho Desk,
    processes them, and saves them locally in JSON format.
    """
    try:
        access_token = get_access_token()
        save_all_articles_to_json(access_token, specific_department_id, specific_category_id)
    except Exception as e:
        logger.exception("Error occurred while fetching or saving articles: %s", e)
