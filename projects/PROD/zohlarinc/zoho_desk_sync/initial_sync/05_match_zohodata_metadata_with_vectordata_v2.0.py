"""
Script to match Zoho Desk article metadata with vector database entries.

This script reads metadata from Zoho Desk articles and matches them with entries
in the vector database based on the title, updating the vector entries with Zoho
article details. The updated vector data is then saved to a new JSON file.

Environment Variables:
- BASE_DIR: Base directory for all files (from .env file)

Input:
- Reads JSON files for Zoho articles and vector data.

Output:
- Saves the updated vector database to a new JSON file.
"""

import json
import os
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
# Load the base directory from environment variables
BASE_DIR = os.getenv('BASE_DIR')

# Paths to JSON files, dynamically constructed using BASE_DIR
zoho_articles_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/04_zohodata_metadata.json')
vector_database_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/02_normalized_vectordata.json')
output_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/05_matched_vectordata.json')

# Read the Zoho Desk articles
with open(zoho_articles_path, 'r', encoding='utf-8') as file:
    zoho_articles = json.load(file)

# Read the vector database
with open(vector_database_path, 'r', encoding='utf-8') as file:
    vector_database = json.load(file)

def map_article_to_vector_entry(article, vector_entry):
    """
    Maps a Zoho article to a vector database entry by updating the vector entry
    with details from the Zoho article.
    """
    updated_entry = {
        "namespace": "",
        "id": article.get("id", ""),
        "title": article.get("title", ""),
        "answer": article.get("answer", ""),
        "link": article.get("webUrl", ""),
        "parent": "",
        "keywords": vector_entry.get("keywords", []),
        "meta_description": article.get("summary", ""),
        "combined_text": vector_entry.get("combined_text", ""),
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
                "category": article.get("category", {}).get("name", ""),
                "permalink": article.get("permalink", ""),
                "categoryId": article.get("categoryId", ""),
                "status": article.get("status", ""),
                "tags": article.get("tags", []),
                "attachments": article.get("attachments", ""),
            }
        }
    }
    return updated_entry

def update_vector_database_with_zoho_response(vector_database, zoho_articles):
    """
    Updates the vector database with details from matching Zoho Desk articles.
    The match is based on the title of the articles.
    """
    for i, vector_entry in enumerate(vector_database):
        matching_article = next((article for article in zoho_articles if article['title'] == vector_entry['title']), None)
        if matching_article:
            updated_entry = map_article_to_vector_entry(matching_article, vector_entry)
            vector_database[i] = updated_entry

# Update the vector database with Zoho article metadata
update_vector_database_with_zoho_response(vector_database, zoho_articles)

# Save the updated vector database to a new JSON file
with open(output_path, 'w', encoding='utf-8') as file:
    json.dump(vector_database, file, indent=4, ensure_ascii=False)

print("The updated vector database has been successfully saved to '05_matched_vectordata.json'.")
