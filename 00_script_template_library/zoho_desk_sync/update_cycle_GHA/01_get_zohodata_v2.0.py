"""
File Manipulation and AWS S3 Storage Details:

This script handles the process of fetching data from the Zoho API and saving it to AWS S3.

Overview:
1. **Data Fetching**: Fetches articles from the Zoho Desk API using OAuth authentication.
2. **Data Storage**: Stores the data as JSON in the specified AWS S3 bucket and tenant directory.

Process Flow:
- Authenticate with Zoho OAuth API to obtain an access token.
- Fetch articles from the Zoho Desk API, paginated by 50 articles per page.
- Filter articles by `departmentId` and `categoryId`.
- Collect and normalize the relevant articles (including converting HTML to plain text).
- Store the collected and processed data in a JSON file on S3 at:
  `s3://<bucket_name>/<TENANT_NAME>/zohodesk-data/01_zohodata.json`.

Environment Variables:
- `S3_BUCKET_NAME`: The AWS S3 bucket name.
- `TENANT_NAME`: The tenant name for organizing files in S3.
- `CLIENT_ID`, `CLIENT_SECRET`, `REDIRECT_URI`, `REFRESH_TOKEN`: Zoho API OAuth credentials.
- `DEPARTMENT_ID`: The department ID for filtering Zoho articles.
- `CATEGORY_ID`: The category ID for filtering Zoho articles.

"""

import logging
import requests
import json
import boto3
import os

from bs4 import BeautifulSoup
import unicodedata
from utils import load_env

# Load environment variables using the helper function
load_env()
# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

# AWS S3 Configuration
s3_client = boto3.client('s3')
bucket_name = os.getenv('S3_BUCKET_NAME')  # Bucket Name from environment variables
storage_prefix = os.getenv('TENANT_NAME') + "/zohodesk-data"
output_path = storage_prefix + '/01_zohodata.json'  # S3 path for the JSON file

# API endpoints and authentication details from environment variables
token_url = os.getenv('TOKEN_URL')
articles_url = os.getenv('ARTICLES_URL')
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
redirect_uri = os.getenv('REDIRECT_URI')
refresh_token = os.getenv('REFRESH_TOKEN')

# Data for the POST request to obtain the access token
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

    This function sends a POST request to Zoho's OAuth API to retrieve an access token using
    the refresh token stored in environment variables.

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
    Converts HTML content to plain text using BeautifulSoup.

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
    Fetches all relevant articles from Zoho Desk and uploads them as a JSON file to S3.

    This function retrieves all published articles from the Zoho Desk API filtered by department ID and category ID.
    It handles pagination, processes the articles in batches, and uploads the result as a JSON file to AWS S3.

    Args:
        access_token (str): The access token for Zoho API.
        department_id (str): The department ID to filter articles by.
        category_id (str): The category ID to filter articles by.

    Raises:
        requests.RequestException: If any request to the Zoho Desk API or AWS S3 fails.
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
        logger.info(f"Fetching articles, page {page}")
        response = requests.get(articles_url, headers=headers, params=params)
        response.raise_for_status()
        articles_data = response.json()

        articles = articles_data.get('data', [])
        if not articles:
            has_more_pages = False
        else:
            page += 1

        for article in articles:
            if article.get('departmentId') == department_id:
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

    # Upload the data to S3 as a JSON file
    s3_client.put_object(Bucket=bucket_name, Key=output_path, Body=json.dumps(collected_articles, ensure_ascii=False, indent=4))
    logger.info(f"Articles successfully uploaded to S3: {bucket_name}/{output_path}")

if __name__ == "__main__":
    """
    Main entry point of the script.

    This section fetches the access token, retrieves all relevant articles from Zoho Desk,
    and saves them in AWS S3 as a JSON file.
    """
    specific_department_id = os.getenv('DEPARTMENT_ID')
    specific_category_id = os.getenv('CATEGORY_ID')

    try:
        access_token = get_access_token()
        save_all_articles_to_json(access_token, specific_department_id, specific_category_id)
    except Exception as e:
        logger.exception("Error occurred while fetching or saving articles: %s", e)
