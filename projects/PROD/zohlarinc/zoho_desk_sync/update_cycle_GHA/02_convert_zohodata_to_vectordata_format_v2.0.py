"""
File Manipulation Details:

This script is responsible for fetching Zoho data from AWS S3, transforming it into a new format suitable for vector-based data structures, and saving the transformed data back to AWS S3.

Process:
1. **Load JSON data from S3**:
   - Bucket: Retrieved from environment variable `S3_BUCKET_NAME`.
   - Key: `<TENANT_NAME>/zohodesk-data/01_zohodata.json`.
   - Uses the boto3 S3 client to load and deserialize the JSON data.

2. **Transform JSON data**:
   - Each article from the loaded JSON is mapped to a new structure that includes fields like 'id', 'title', 'summary', 'webUrl', and more.

3. **Save transformed data back to S3**:
   - Bucket: Same as input (`S3_BUCKET_NAME`).
   - Key: `<TENANT_NAME>/zohodesk-data/02_converted_zohodata.json`.
   - Transformed data is serialized to JSON format and uploaded to S3.
"""

import json
import boto3
import logging
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
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

# AWS S3 Configuration
s3_client = boto3.client('s3')
bucket_name = os.getenv('S3_BUCKET_NAME')
storage_prefix = os.getenv('TENANT_NAME') + "/zohodesk-data"
input_key = f"{storage_prefix}/01_zohodata.json"
output_key = f"{storage_prefix}/02_converted_zohodata.json"

def load_json_from_s3(bucket, key):
    """
    Loads JSON data from a specified S3 bucket and key.

    Args:
        bucket (str): The name of the S3 bucket.
        key (str): The S3 key (path) of the JSON file to be loaded.

    Returns:
        dict or list: The deserialized JSON content from S3.

    Raises:
        Exception: If there's an error loading or parsing the data.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.load(response['Body'])
        logger.info(f"Data successfully loaded from S3: {bucket}/{key}")
        return data
    except Exception as e:
        logger.error(f"Failed to load data from S3: {e}")
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
        The normalized data structure.
    """
    if isinstance(data, str):
        return unicodedata.normalize('NFKC', data)
    elif isinstance(data, list):
        return [normalize_unicode_characters(item) for item in data]
    elif isinstance(data, dict):
        return {key: normalize_unicode_characters(value) for key, value in data.items()}
    else:
        return data

def map_article_to_vector_entry(article):
    """
    Maps a Zoho article to the target vector data structure.

    Args:
        article (dict): A Zoho article object.

    Returns:
        dict: A transformed vector data entry.
    """
    if 'answer' in article:
        article['answer'] = html_to_plain_text(article['answer'])
    
    article = normalize_unicode_characters(article)

    vector_entry = {
        "namespace": "",
        "id": article["id"],
        "title": article["title"],
        "answer": article["answer"],
        "link": article["webUrl"],
        "parent": "",
        "keywords": "",
        "meta_description": article["summary"],
        "combined_text": "",
        "metadata": {
            "category": article.get("category", {}).get("name", ""),
            "sub_category": "",
            "tags": article.get("tags", []),
            "last_updated": article["modifiedTime"],
            "author": article.get("author", {}).get("name", ""),
            "views": article["viewCount"],
            "like": article["likeCount"],
            "difficulty_level": "",
            "version": article["latestVersion"],
            "related_links": [],
            "zd_metadata": {
                "modifiedTime": article["modifiedTime"],
                "departmentId": article["departmentId"],
                "creatorId": article["creatorId"],
                "dislikeCount": article["dislikeCount"],
                "modifierId": article["modifierId"],
                "likeCount": article["likeCount"],
                "locale": article["locale"],
                "ownerId": article["ownerId"],
                "title": article["title"],
                "translationState": article["translationState"],
                "isTrashed": article["isTrashed"],
                "createdTime": article["createdTime"],
                "modifiedBy": article["modifiedBy"],
                "id": article["id"],
                "viewCount": article["viewCount"],
                "translationSource": article["translationSource"],
                "owner": article["owner"],
                "summary": article["summary"],
                "latestVersionStatus": article["latestVersionStatus"],
                "author": article["author"],
                "permission": article["permission"],
                "authorId": article["authorId"],
                "usageCount": article["usageCount"],
                "commentCount": article["commentCount"],
                "rootCategoryId": article["rootCategoryId"],
                "sourceLocale": article["sourceLocale"],
                "translationId": article["translationId"],
                "createdBy": article["createdBy"],
                "latestVersion": article["latestVersion"],
                "webUrl": article["webUrl"],
                "feedbackCount": article["feedbackCount"],
                "portalUrl": article["portalUrl"],
                "attachmentCount": article["attachmentCount"],
                "latestPublishedVersion": article["latestPublishedVersion"],
                "position": article["position"],
                "availableLocaleTranslations": article["availableLocaleTranslations"],
                "category": article["category"],
                "permalink": article["permalink"],
                "categoryId": article["categoryId"],
                "status": article["status"],
                "tags": article.get("tags", []),
                "attachments": article.get("attachments", ""),
            }
        }
    }

    return vector_entry

def save_json_to_s3(data, bucket, key):
    """
    Saves the given data as a JSON file to the specified S3 bucket and key.

    Args:
        data: The data to be serialized to JSON and saved.
        bucket (str): The name of the S3 bucket.
        key (str): The S3 key (path) where the data will be saved.

    Raises:
        Exception: If the save operation fails.
    """
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(data, ensure_ascii=False, indent=4))
        logger.info(f"Data successfully saved to S3: {bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to save data to S3: {e}")
        raise

def main():
    """
    Main function that handles the entire process:
    - Fetch Zoho articles from S3.
    - Transform them to the vector data format.
    - Save the transformed data back to S3.
    """
    try:
        zoho_articles = load_json_from_s3(bucket_name, input_key)
        transformed_articles = [map_article_to_vector_entry(article) for article in zoho_articles]
        save_json_to_s3(transformed_articles, bucket_name, output_key)
        logger.info(f"Transformed data successfully saved to {output_key} on S3.")
    except Exception as e:
        logger.error(f"Error during processing: {e}")

if __name__ == "__main__":
    main()
