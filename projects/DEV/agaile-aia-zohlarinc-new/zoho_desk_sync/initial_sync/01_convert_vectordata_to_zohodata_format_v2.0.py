"""
Script to convert vector data format to Zoho Desk data format.

This script reads a JSON file with enriched vector data, transforms it into the format required
by Zoho Desk, and saves the converted data to a new JSON file.

Environment Variables:
- BASE_DIR: Base directory for all files (from .env file)
- DEPARTMENT_ID: Zoho Desk department ID (from .env file)
- CATEGORY_ID: Zoho Desk category ID (from .env file)
- PERMISSIONS: Permissions for the articles (from .env file)
- TAG: Custom tags to be added (from .env file)

Input:
- JSON file containing vector data (input file path constructed dynamically).

Output:
- JSON file with data formatted for Zoho Desk (output file path constructed dynamically).
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

# Load base directory and other environment variables
BASE_DIR = os.getenv('BASE_DIR')  # Base directory from .env file
DEPARTMENT_ID = os.getenv('DEPARTMENT_ID')
CATEGORY_ID = os.getenv('CATEGORY_ID')
PERMISSIONS = os.getenv('PERMISSIONS')
TAG = os.getenv('TAG')

def transform_data(old_data):
    """
    Transforms vector data format into Zoho Desk data format.

    Parameters:
    - old_data (dict): Dictionary representing an article in the vector data format.

    Returns:
    - new_data (dict): Transformed dictionary in Zoho Desk format.
    """
    new_data = {
        "namespace": old_data.get("namespace", ""),
        "id": old_data.get("id", ""),
        "title": old_data.get("title", "required"),
        "answer": old_data.get("content", "required"),  # 'content' converted to 'answer'
        "link": old_data.get("link", ""),
        "parent": old_data.get("parent", ""),
        "keywords": ", ".join(old_data.get("keywords", [])),  # Join keywords into a comma-separated string
        "meta_description": old_data.get("meta_description", ""),
        "combined_text": old_data.get("combined_text", ""),
        "metadata": {
            "category": old_data["metadata"].get("category", ""),
            "sub_category": old_data["metadata"].get("sub_category", ""),
            "tags": old_data["metadata"].get("tags", []),
            "last_updated": old_data["metadata"].get("last_updated", ""),
            "author": old_data["metadata"].get("author", ""),
            "views": "",
            "like": "",
            "difficulty_level": old_data["metadata"].get("difficulty_level", ""),
            "version": old_data["metadata"].get("version", ""),
            "related_links": old_data["metadata"].get("related_links", []),
            "zd_metadata": {
                "modifiedTime": "",
                "departmentId": DEPARTMENT_ID,
                "creatorId": "",
                "dislikeCount": "",
                "modifierId": "",
                "likeCount": "",
                "locale": "",
                "ownerId": "",
                "title": old_data.get("title", "required"),
                "translationState": "",
                "isTrashed": "",
                "createdTime": "",
                "modifiedBy": {
                    "photoURL": "",
                    "name": "",
                    "id": "",
                    "status": "",
                    "zuid": ""
                },
                "id": "",
                "viewCount": "",
                "translationSource": "",
                "owner": {
                    "photoURL": "",
                    "name": "",
                    "id": "",
                    "status": "",
                    "zuid": ""
                },
                "summary": "",
                "latestVersionStatus": "",
                "author": {
                    "photoURL": "",
                    "name": "",
                    "id": "",
                    "status": "",
                    "zuid": ""
                },
                "permission": PERMISSIONS,
                "authorId": "",
                "usageCount": "",
                "commentCount": "",
                "rootCategoryId": "",
                "sourceLocale": "",
                "translationId": "",
                "createdBy": {
                    "photoURL": "",
                    "name": "",
                    "id": "",
                    "status": "",
                    "zuid": ""
                },
                "latestVersion": "",
                "webUrl": "",
                "feedbackCount": "",
                "portalUrl": "",
                "attachmentCount": "",
                "latestPublishedVersion": "",
                "position": "",
                "availableLocaleTranslations": [],
                "category": {
                    "name": "",
                    "id": CATEGORY_ID,
                    "locale": ""
                },
                "permalink": "",
                "categoryId": CATEGORY_ID,
                "status": "Published",
                "tags": [TAG],
                "attachments": []
            }
        }
    }
    return new_data

def main():
    """
    Main function to read the vector data, transform it, and save the transformed data.
    """
    # Construct file paths using BASE_DIR
    input_file_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/your-data.json')
    output_file_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/01_converted_vectordata.json')

    try:
        # Load the vector data
        with open(input_file_path, 'r') as file:
            old_data_list = json.load(file)

        # Transform each article in the vector data
        new_data_list = [transform_data(old_data) for old_data in old_data_list]

        # Save the transformed data to the output file
        with open(output_file_path, 'w') as file:
            json.dump(new_data_list, file, indent=4)

        print("Data transformation completed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
