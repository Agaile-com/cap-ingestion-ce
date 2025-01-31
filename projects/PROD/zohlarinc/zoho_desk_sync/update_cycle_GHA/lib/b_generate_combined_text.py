import json
import os
import logging
from tqdm import tqdm

import unicodedata
import boto3
import sys
from pathlib import Path

# Dynamically add the parent directory (zoho_desk_sync) to sys.path
script_dir = Path(__file__).resolve().parent
parent_dir = script_dir.parent  # This will resolve to zoho_desk_sync
sys.path.append(str(parent_dir))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# AWS S3 Configuration
s3_client = boto3.client('s3')
bucket_name = os.getenv('S3_BUCKET_NAME')

# Base directory from environment for local file reference
BASE_DIR = os.getenv('BASE_DIR')

def normalize_unicode_characters(data):
    """
    Recursive function to normalize all strings in a nested object (dictionary or list).
    """
    if isinstance(data, str):
        return unicodedata.normalize('NFKC', data)
    elif isinstance(data, list):
        return [normalize_unicode_characters(item) for item in data]
    elif isinstance(data, dict):
        return {key: normalize_unicode_characters(value) for key, value in data.items()}
    else:
        return data

def create_combined_text(entry):
    """
    Create a combined text string from the title and keywords of an entry, ensuring uniqueness and order.
    """
    try:
        elements = [entry['title'], *entry.get('keywords', [])]
        combined_text_lowercase = ' '.join(elements).lower()
        unique_words = sorted(set(combined_text_lowercase.split()), key=combined_text_lowercase.index)
        combined_text_with_spaces = ' '.join(unique_words).replace(',', ', ')
        return combined_text_with_spaces
    except KeyError as e:
        logging.error(f"Error in entry: {e}")
        return ""

def transform_structure(old_content):
    """
    Reconstruct the full structure with additional combined text.
    """
    new_content = {
                "namespace": "",
                    "id": old_content.get("id"),
                    "title": old_content.get('title', ''),
                    "answer": old_content.get('answer', ''),
                    "link": old_content.get("metadata", {}).get("zd_metadata", {}).get("webUrl"),
                    "parent": "",
                    "keywords": old_content.get('keywords', ''),
                    "meta_description": old_content.get("metadata", {}).get("zd_metadata", {}).get("summary"),
                    "combined_text": create_combined_text(old_content),
                    "metadata": {
                        "category": old_content.get("metadata", {}).get("category", ""),
                        "sub_category": old_content.get("metadata", {}).get("sub_category", ""),
                        "tags": old_content.get("metadata", {}).get("tags", []),
                        "last_updated": old_content.get("metadata", {}).get("zd_metadata", {}).get("modifiedTime"),
                        "author": old_content.get("metadata", {}).get("zd_metadata", {}).get("author", {}).get("name", ""),
                        "views": old_content.get("metadata", {}).get("zd_metadata", {}).get("viewCount"),
                        "like": old_content.get("metadata", {}).get("zd_metadata", {}).get("likeCount"),
                        "difficulty_level": old_content.get("metadata", {}).get("difficulty_level", ""),
                        "version": old_content.get("metadata", {}).get("zd_metadata", {}).get("latestVersion"),
                        "related_links": old_content.get("metadata", {}).get("related_links", []),
                        "zd_metadata": {
                            "modifiedTime": old_content.get("metadata", {}).get("zd_metadata", {}).get("modifiedTime"),
                            "departmentId": old_content.get("metadata", {}).get("zd_metadata", {}).get("departmentId"),
                            "creatorId": old_content.get("metadata", {}).get("zd_metadata", {}).get("creatorId"),
                            "dislikeCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("dislikeCount"),
                            "modifierId": old_content.get("metadata", {}).get("zd_metadata", {}).get("modifierId"),
                            "likeCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("likeCount"),
                            "locale": old_content.get("metadata", {}).get("zd_metadata", {}).get("locale"),
                            "ownerId": old_content.get("metadata", {}).get("zd_metadata", {}).get("ownerId"),
                            "title": old_content.get("metadata", {}).get("zd_metadata", {}).get("title"),
                            "translationState": old_content.get("metadata", {}).get("zd_metadata", {}).get("translationState"),
                            "isTrashed": old_content.get("metadata", {}).get("zd_metadata", {}).get("isTrashed"),
                            "createdTime": old_content.get("metadata", {}).get("zd_metadata", {}).get("createdTime"),
                            "modifiedBy": old_content.get("metadata", {}).get("zd_metadata", {}).get("modifiedBy"),
                            "id": old_content.get("metadata", {}).get("zd_metadata", {}).get("id"),
                            "viewCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("viewCount"),
                            "translationSource": old_content.get("metadata", {}).get("zd_metadata", {}).get("translationSource"),
                            "owner": old_content.get("metadata", {}).get("zd_metadata", {}).get("owner"),
                            "summary": old_content.get("metadata", {}).get("zd_metadata", {}).get("summary"),
                            "latestVersionStatus": old_content.get("metadata", {}).get("zd_metadata", {}).get("latestVersionStatus"),
                            "author": old_content.get("metadata", {}).get("zd_metadata", {}).get("author"),
                            "permission": old_content.get("metadata", {}).get("zd_metadata", {}).get("permission"),
                            "authorId": old_content.get("metadata", {}).get("zd_metadata", {}).get("authorId"),
                            "usageCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("usageCount"),
                            "commentCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("commentCount"),
                            "rootCategoryId": old_content.get("metadata", {}).get("zd_metadata", {}).get("rootCategoryId"),
                            "sourceLocale": old_content.get("metadata", {}).get("zd_metadata", {}).get("sourceLocale"),
                            "translationId": old_content.get("metadata", {}).get("zd_metadata", {}).get("translationId"),
                            "createdBy": old_content.get("metadata", {}).get("zd_metadata", {}).get("createdBy"),
                            "latestVersion": old_content.get("metadata", {}).get("zd_metadata", {}).get("latestVersion"),
                            "webUrl": old_content.get("metadata", {}).get("zd_metadata", {}).get("webUrl"),
                            "feedbackCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("feedbackCount"),
                            "portalUrl": old_content.get("metadata", {}).get("zd_metadata", {}).get("portalUrl"),
                            "attachmentCount": old_content.get("metadata", {}).get("zd_metadata", {}).get("attachmentCount"),
                            "latestPublishedVersion": old_content.get("metadata", {}).get("zd_metadata", {}).get("latestPublishedVersion"),
                            "position": old_content.get("metadata", {}).get("zd_metadata", {}).get("position"),
                            "availableLocaleTranslations": old_content.get("metadata", {}).get("zd_metadata", {}).get("availableLocaleTranslations", []),
                            "category": old_content.get("metadata", {}).get("zd_metadata", {}).get("category"),
                            "permalink": old_content.get("metadata", {}).get("zd_metadata", {}).get("permalink"),
                            "categoryId": old_content.get("metadata", {}).get("zd_metadata", {}).get("categoryId"),
                            "status": old_content.get("metadata", {}).get("zd_metadata", {}).get("status"),
                            "tags": old_content.get("metadata", {}).get("zd_metadata", {}).get("tags", []),
                            "attachments": old_content.get("metadata", {}).get("zd_metadata", {}).get("attachments", []),
                        }
                    }
                }


    return new_content

def enrich_with_combined_text(bucket_name, input_key, output_key):
    """
    Load data from S3, enrich it with combined text, and save it back to S3.
    """
    try:
        # Load data from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=input_key)
        old_contents = json.load(response['Body'])

        # Process each entry and update with combined text
        new_contents = [transform_structure(content) for content in tqdm(old_contents, desc="Processing Records")]
        normalized_contents = normalize_unicode_characters(new_contents)

        # Save the processed data back to S3
        s3_client.put_object(Bucket=bucket_name, Key=output_key, Body=json.dumps(normalized_contents, indent=4))
        logging.info(f"Processed data successfully saved back to S3: {output_key}")

    except Exception as e:
        logging.error(f"Error processing data from S3: {e}")

if __name__ == "__main__":
    # Input and output paths are relative to the BASE_DIR
    input_key = os.path.join(BASE_DIR, 'zoho_desk_sync/update_cycle_GHA/data/enriched/04_synced_vectordata_without_combined_text.json')
    output_key = os.path.join(BASE_DIR, 'zoho_desk_sync/update_cycle_GHA/data/enriched/04_synced_vectordata_with_combined_text.json')

    # Enrich with combined text and save back to S3
    enrich_with_combined_text(bucket_name, input_key, output_key)