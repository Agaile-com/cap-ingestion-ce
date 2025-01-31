"""
Script to convert Zoho Desk article data into vector data format.

This script reads Zoho Desk articles from a JSON file, converts them to a structured dictionary
format suitable for the vector database, and saves the converted data to a new JSON file.

Environment Variables:
- BASE_DIR: Base directory for all files (from .env file)

Input:
- Reads a JSON file containing Zoho Desk articles.

Output:
- Saves the converted articles in vector database format to a new JSON file.
"""

import json
import os
from utils import load_env

# Load environment variables using the helper function
load_env()

# Load base directory from environment
BASE_DIR = os.getenv('BASE_DIR')

# Paths to the input and output JSON files, constructed using BASE_DIR
zoho_articles_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/06_zohodata.json')
output_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/07_converted_zohodata.json')

# Read the Zoho Desk articles from the input JSON file
with open(zoho_articles_path, 'r', encoding='utf-8') as file:
    zoho_articles = json.load(file)

def map_article_to_vector_entry(article):
    """
    Converts a Zoho Desk article into a vector database entry.

    Parameters:
    - article (dict): A Zoho Desk article dictionary.

    Returns:
    - vector_entry (dict): A structured dictionary in vector database format.
    """
    vector_entry = {
        "namespace": "",
        "id": article.get("id", ""),
        "title": article.get("title", ""),
        "answer": article.get("answer", ""),
        "link": article.get("webUrl", ""),
        "parent": "",
        "keywords": "",
        "meta_description": article.get("summary", ""),
        "combined_text": "",
        "metadata": {
            "category": article.get("category", {}).get("name", ""),
            "sub_category": "",
            "tags": article.get("tags", []),
            "last_updated": article.get("modifiedTime", ""),
            "author": article.get("author", {}).get("name", ""),
            "views": article.get("viewCount", ""),
            "like": article.get("likeCount", ""),
            "difficulty_level": "",
            "version": article.get("latestVersion", ""),
            "related_links": [],
            "zd_metadata": {
                "modifiedTime": article.get("modifiedTime", ""),
                "departmentId": article.get("departmentId", ""),
                "creatorId": article.get("creatorId", ""),
                "dislikeCount": article.get("dislikeCount", ""),
                "modifierId": article.get("modifierId", ""),
                "likeCount": article.get("likeCount", ""),
                "locale": article.get("locale", ""),
                "ownerId": article.get("ownerId", ""),
                "title": article.get("title", ""),
                "translationState": article.get("translationState", ""),
                "isTrashed": article.get("isTrashed", ""),
                "createdTime": article.get("createdTime", ""),
                "modifiedBy": article.get("modifiedBy", {}),
                "id": article.get("id", ""),
                "viewCount": article.get("viewCount", ""),
                "translationSource": article.get("translationSource", ""),
                "owner": article.get("owner", {}),
                "summary": article.get("summary", ""),
                "latestVersionStatus": article.get("latestVersionStatus", ""),
                "author": article.get("author", {}),
                "permission": article.get("permission", ""),
                "authorId": article.get("authorId", ""),
                "usageCount": article.get("usageCount", ""),
                "commentCount": article.get("commentCount", ""),
                "rootCategoryId": article.get("rootCategoryId", ""),
                "sourceLocale": article.get("sourceLocale", ""),
                "translationId": article.get("translationId", ""),
                "createdBy": article.get("createdBy", {}),
                "latestVersion": article.get("latestVersion", ""),
                "webUrl": article.get("webUrl", ""),
                "feedbackCount": article.get("feedbackCount", ""),
                "portalUrl": article.get("portalUrl", ""),
                "attachmentCount": article.get("attachmentCount", ""),
                "latestPublishedVersion": article.get("latestPublishedVersion", ""),
                "position": article.get("position", ""),
                "availableLocaleTranslations": article.get("availableLocaleTranslations", []),
                "category": article.get("category", {}),
                "permalink": article.get("permalink", ""),
                "categoryId": article.get("categoryId", ""),
                "status": article.get("status", ""),
                "tags": article.get("tags", []),
                "attachments": article.get("attachments", ""),
            }
        }
    }
    return vector_entry

# Convert each Zoho Desk article to the vector database format
transformed_articles = [map_article_to_vector_entry(article) for article in zoho_articles]

# Save the transformed articles to the output JSON file
with open(output_path, 'w', encoding='utf-8') as file:
    json.dump(transformed_articles, file, indent=4, ensure_ascii=False)

print(f"The transformed data has been successfully saved to '{output_path}'.")
