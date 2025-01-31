"""
Zoho Desk Article Deletion Script

This script fetches all articles from Zoho Desk and moves them to the trash if they match the specified departmentId and categoryId.
It can also use an existing JSON file with article IDs to move the articles to trash.

File Paths:
- Article IDs to be fetched and saved to a JSON file: 'AGAILE-ZOHO-SYNC-DEV/zoho_desk_sync/initial_sync/data/articles_to_delete.json'
- Existing article IDs file (optional): 'AGAILE-ZOHO-SYNC-DEV/zoho_desk_sync/initial_sync/data/articles_to_delete.json'

API Endpoints:
- Zoho OAuth token URL: 'https://accounts.zoho.com/oauth/v2/token'
- Zoho Desk articles URL: 'https://desk.zoho.com/api/v1/articles'
- Move to trash URL: 'https://desk.zoho.com/api/v1/articles/moveToTrash'

Environment variables:
- DEPARTMENT_ID, CATEGORY_ID, CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, REFRESH_TOKEN
"""

import logging
import requests
import json
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Laden der Umgebungsvariablen aus der .env-Datei
load_dotenv()

# Configure logging (simplified for GitHub Actions)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API and authentication details from the .env file
DEPARTMENT_ID = os.getenv('DEPARTMENT_ID')
CATEGORY_ID = os.getenv('CATEGORY_ID')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
ORG_ID = os.getenv('ORG_ID')

# Base directory from .env file
BASE_DIR = Path(os.getenv('BASE_DIR'))

# Paths for the articles file (base directory joined with the relative path)
OUTPUT_FILE_PATH = BASE_DIR / 'zoho_desk_sync/initial_sync/data/articles_to_delete.json'
EXISTING_ARTICLES_FILE_PATH = BASE_DIR / 'zoho_desk_sync/initial_sync/data/articles_to_delete.json'

# API endpoints and authentication details
token_url = 'https://accounts.zoho.com/oauth/v2/token'
articles_url = 'https://desk.zoho.com/api/v1/articles'
move_to_trash_url = 'https://desk.zoho.com/api/v1/articles/moveToTrash'

# Data for the OAuth request
data = {
    'grant_type': 'refresh_token',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'redirect_uri': REDIRECT_URI,
    'refresh_token': REFRESH_TOKEN
}

def get_access_token():
    """
    Retrieves the access token from Zoho OAuth API.
    """
    try:
        logging.info("Requesting access token from Zoho OAuth API...")
        response = requests.post(token_url, headers={'Content-Type': 'application/x-www-form-urlencoded'}, data=data)
        response.raise_for_status()
        access_token = response.json().get('access_token')
        if not access_token:
            logging.error("No access token received.")
            raise ValueError("No access token received.")
        logging.info("Access token received successfully.")
        return access_token
    except requests.RequestException as e:
        logging.error(f"Error requesting access token: {e}")
        raise

def save_all_article_ids_to_json(access_token, output_path, department_id, category_id):
    """
    Fetches all article IDs from Zoho Desk and saves them to a JSON file.
    Filters by departmentId and categoryId.
    """
    collected_article_ids = []
    headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
    params = {'limit': 50}
    page = 1
    has_more_pages = True

    while has_more_pages:
        logging.info(f"Fetching articles, page {page}")
        response = requests.get(articles_url, headers=headers, params=params)
        if response.status_code == 401:
            logging.error("Unauthorized access - check your credentials and tokens.")
            raise requests.exceptions.HTTPError("401 Unauthorized")
        response.raise_for_status()
        articles_data = response.json()
        articles = articles_data.get('data', [])

        for article in articles:
            if article.get('departmentId') == department_id and article.get('categoryId') == category_id:
                collected_article_ids.append(article['id'])

        if articles:
            page += 1
            params['from'] = page * params['limit']
        else:
            has_more_pages = False

    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(collected_article_ids, file, indent=4, ensure_ascii=False)
    logging.info(f"Article IDs successfully saved. Total articles: {len(collected_article_ids)}")

def move_articles_to_trash(access_token, article_ids):
    """
    Moves the given list of article IDs to the trash in Zoho Desk.
    """
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }
    data = {'ids': article_ids}
    
    try:
        logging.info(f"Moving article IDs to trash: {article_ids}")
        response = requests.post(move_to_trash_url, headers=headers, json=data)
        response.raise_for_status()
        logging.info("Articles successfully moved to trash.")
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error moving articles to trash: {e}")
        raise

def main():
    """
    Main function to fetch articles from Zoho Desk and move them to trash.
    """
    try:
        # Get access token
        access_token = get_access_token()

        # Check if the user wants to use an existing file for article IDs
        use_existing = input("Use existing file with article IDs? (yes/no): ").strip().lower()
        if use_existing == 'yes':
            if os.path.exists(EXISTING_ARTICLES_FILE_PATH):
                with open(EXISTING_ARTICLES_FILE_PATH, 'r', encoding='utf-8') as file:
                    article_ids = json.load(file)
                logging.info(f"Using existing article IDs from {EXISTING_ARTICLES_FILE_PATH}")
            else:
                logging.error(f"The file {EXISTING_ARTICLES_FILE_PATH} does not exist.")
                return
        else:
            save_all_article_ids_to_json(access_token, OUTPUT_FILE_PATH, DEPARTMENT_ID, CATEGORY_ID)
            with open(OUTPUT_FILE_PATH, 'r', encoding='utf-8') as file:
                article_ids = json.load(file)

        # Process articles in batches of 50 to move them to trash
        for i in range(0, len(article_ids), 50):
            batch_ids = article_ids[i:i+50]
            move_articles_to_trash(access_token, batch_ids)

    except Exception as e:
        logging.exception(f"Error moving articles to trash: {e}")

if __name__ == "__main__":
    main()
