"""
Script to enrich vector data with keywords using the dspy library and OpenAI.

This script loads vector data from an S3 bucket, enriches it with keywords generated via OpenAI (through the dspy library),
and then saves the processed data back to the S3 bucket.

Environment Variables:
- BASE_DIR: Base directory for all local files (from .env file)
- S3_BUCKET_NAME: The name of the S3 bucket for uploading/downloading files (from .env file)
- OPENAI_API_KEY: API key for accessing OpenAI models via dspy (from .env file)

Input:
- S3 object containing vector data.

Output:
- Enriched vector data saved to a new S3 object.
"""

import json
import os
import logging
from tqdm import tqdm
import dspy

import unicodedata
import boto3
import sys
from pathlib import Path

# Dynamically add the parent directory (zoho_desk_sync) to sys.path
script_dir = Path(__file__).resolve().parent
parent_dir = script_dir.parent  # This will resolve to zoho_desk_sync
sys.path.append(str(parent_dir))

BASE_DIR = os.getenv('BASE_DIR')
# AWS S3 Configuration
s3_client = boto3.client('s3')
bucket_name = os.getenv('S3_BUCKET_NAME')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configure dspy with OpenAI GPT-4
dspy.configure(lm=dspy.OpenAI(model='gpt-4', api_key=os.getenv('OPENAI_API_KEY')))

# Normalization function for Unicode characters
def normalize_unicode_characters(data):
    """
    Recursively normalize all string characters in a nested JSON object using Unicode NFKC normalization.
    """
    if isinstance(data, str):
        return unicodedata.normalize('NFKC', data)
    elif isinstance(data, list):
        return [normalize_unicode_characters(item) for item in data]
    elif isinstance(data, dict):
        return {key: normalize_unicode_characters(value) for key, value in data.items()}
    else:
        return data

# dspy model for extracting keywords
class ExtractKeywords(dspy.Signature):
    """Extract up to two relevant keywords from the content."""
    content = dspy.InputField()
    keywords = dspy.OutputField()

extract_keywords_model = dspy.Predict(ExtractKeywords)

# Function to transform the structure of the content with keywords enrichment
def transform_structure(old_content):
    """
    Transforms the old content structure by adding keywords generated via OpenAI.
    """
    keywords_response = extract_keywords_model(content=old_content['answer'])
    keywords = keywords_response.keywords.split(', ')  # Assume keywords are returned as a comma-separated string

    new_content = {
        "namespace": "",
        "id": old_content.get("id"),
        "title": old_content.get('title', ''),
        "answer": old_content.get('answer', ''),
        "link": old_content.get("metadata", {}).get("zd_metadata", {}).get("webUrl"),
        "parent": "",
        "keywords": keywords,
        "meta_description": old_content.get("metadata", {}).get("zd_metadata", {}).get("summary"),
        "combined_text": old_content.get("combined_text", ""),
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

# Function to enrich vector data with keywords and save it back to S3
def enrich_with_keywords(bucket_name, input_key, output_key):
    """
    Enriches the vector data with keywords using the dspy model and saves it to S3.

    :param bucket_name: Name of the S3 bucket
    :param input_key: S3 object key for input data
    :param output_key: S3 object key for output data
    """
    try:
        # Load data from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=input_key)
        old_contents = json.load(response['Body'])

        # Process each data entry
        new_contents = [transform_structure(content) for content in tqdm(old_contents, desc="Processing records")]
        normalized_contents = normalize_unicode_characters(new_contents)

        # Save processed data back to S3
        s3_client.put_object(Bucket=bucket_name, Key=output_key, Body=json.dumps(normalized_contents, indent=4))
        logging.info(f"Processed data successfully saved to S3: {output_key}")

    except Exception as e:
        logging.error(f"Error processing data: {e}")

# Main function to trigger the enrichment process
def main():
    # Example S3 keys (dynamically generated paths should be used in production)
    input_key = os.path.join(BASE_DIR,'zoho_desk_sync/update_cycle_GHA/data/enriched/04_synced_vectordata_without_both.json')
    output_key = os.path.join(BASE_DIR,'zoho_desk_sync/update_cycle_GHA/data/enriched/04_synced_vectordata_with_keywords.json')

    # Enrich vector data with keywords and upload to S3
    enrich_with_keywords(bucket_name, input_key, output_key)

if __name__ == "__main__":
    main()
