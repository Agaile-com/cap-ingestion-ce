"""
Script to create Zoho Desk articles from normalized vector data.

This script reads a JSON file with normalized vector data, validates and transforms the data, 
and creates articles in Zoho Desk using asynchronous requests with retries for robustness.

Environment Variables:
- BASE_DIR: Base directory for all files (from .env file)
- CLIENT_ID: Zoho OAuth client ID (from .env file)
- CLIENT_SECRET: Zoho OAuth client secret (from .env file)
- REDIRECT_URI: Zoho OAuth redirect URI (from .env file)
- REFRESH_TOKEN: Zoho OAuth refresh token (from .env file)
- ZOHO_TOKEN_URL: Zoho OAuth token URL (from .env file)
- ZOHO_ARTICLES_URL: Zoho Desk articles API URL (from .env file)

Input:
- JSON file with normalized vector data (input file path constructed dynamically).

Output:
- Creates articles in Zoho Desk.
"""

import logging
import requests
from tqdm import tqdm
import json
import random

import os
import asyncio
import aiohttp
from aiohttp import ClientSession, ClientError
from asyncio import Semaphore
import unicodedata
import time
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
# Load configuration from environment variables
BASE_DIR = os.getenv('BASE_DIR')
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
redirect_uri = os.getenv('REDIRECT_URI')
refresh_token = os.getenv('REFRESH_TOKEN')

# Zoho API URLs loaded from .env
token_url = os.getenv('ZOHO_TOKEN_URL')
articles_url = os.getenv('ZOHO_ARTICLES_URL')

# Logging configuration
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

# Maximum retries and exponential backoff factor
MAX_RETRIES = 5
RETRY_BACKOFF_FACTOR = 2

# Function to get access token from Zoho
async def get_access_token(session):
    data = {
        'grant_type': 'refresh_token',
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': redirect_uri,
        'refresh_token': refresh_token
    }
    try:
        logger.info("Requesting access token from Zoho OAuth API...")
        async with session.post(token_url, data=data) as response:
            response.raise_for_status()
            token_response = await response.json()
            access_token = token_response.get('access_token')
            logger.info("Access token received successfully.")
            return access_token
    except ClientError as e:
        logger.error(f"Error requesting Zoho OAuth API: {e}")
        raise

# Function to normalize Unicode characters in JSON data
def normalize_unicode_characters(data):
    if isinstance(data, str):
        return unicodedata.normalize('NFKC', data)
    elif isinstance(data, list):
        return [normalize_unicode_characters(item) for item in data]
    elif isinstance(data, dict):
        return {key: normalize_unicode_characters(value) for key, value in data.items()}
    else:
        return data

# Function to load and normalize the JSON data
def load_and_transform_json(json_file_path):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            vectorjson_data = json.load(file)
        logger.debug("JSON data successfully loaded and transformed.")
        return normalize_unicode_characters(vectorjson_data)
    except Exception as e:
        logger.error(f"Error loading JSON file: {e}")
        raise

# Function to validate article data before sending to Zoho
def validate_article_data(article_data):
    required_fields = ["categoryId", "title", "answer", "status", "permission"]
    for field in required_fields:
        if field not in article_data or not article_data[field]:
            return False
    if "permalink" in article_data and not article_data["permalink"]:
        return False
    return True

# Function to create an article in Zoho Desk
async def create_article(session, access_token, article_data, semaphore):
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }
    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                logger.debug("Attempting to create article in Zoho Desk...")
                article_data_normalized = normalize_unicode_characters(article_data)
                logger.debug(f"Article data: {json.dumps(article_data_normalized, indent=2)}")
                async with session.post(articles_url, headers=headers, json=article_data_normalized) as response:
                    response.raise_for_status()
                    logger.info("Article created successfully in Zoho Desk.")
                    return await response.json()
            except ClientError as e:
                logger.error(f"Error creating article in Zoho Desk: {e}")
                if response.status in {429, 500, 502, 503, 504}:  # Retry on these status codes
                    wait_time = RETRY_BACKOFF_FACTOR ** attempt
                    logger.warning(f"Waiting {wait_time} seconds before retrying...")
                    await asyncio.sleep(wait_time)
                else:
                    if response.content:
                        logger.error(f"Response content: {await response.text()}")
                    raise
        logger.error(f"Max retries reached for article: {article_data}")

# Function to process articles from JSON and create them in Zoho Desk
async def process_articles(json_file_path, sample_size=None):
    async with ClientSession() as session:
        access_token = await get_access_token(session)
        vectorjson_data = load_and_transform_json(json_file_path)

        if sample_size is not None:
            vectorjson_data = random.sample(vectorjson_data, min(sample_size, len(vectorjson_data)))

        semaphore = Semaphore(3)  # Limit concurrent requests to 3

        tasks = []
        for article in tqdm(vectorjson_data, desc="Processing articles"):
            try:
                zd_metadata = article["metadata"]["zd_metadata"]
                article_data = {
                    "categoryId": zd_metadata["categoryId"],  
                    "title": article["title"],
                    "answer": article["answer"],  
                    "status": zd_metadata["status"],
                    "permission": zd_metadata["permission"],
                    "tags": zd_metadata.get("tags", [])
                }
                
                # Validate article data
                if not validate_article_data(article_data):
                    logger.error(f"Invalid article data: {article_data}")
                    continue
                
                tasks.append(create_article(session, access_token, article_data, semaphore))
                await asyncio.sleep(1)  # Add a 1-second delay between requests
            except KeyError as ke:
                logger.error(f"Missing keys in article data: {ke}")
            except Exception as e:
                logger.error(f"Error processing an article: {e}")
        
        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    # Construct file path for the input JSON file using BASE_DIR
    json_file_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/02_normalized_vectordata.json')
    
    sample_size = None  # Set sample_size to None to process all articles
    
    # Start the asynchronous event loop
    asyncio.run(process_articles(json_file_path, sample_size))
