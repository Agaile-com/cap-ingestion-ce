"""
Script to normalize Unicode characters in a JSON file.

This script reads a JSON file containing vector data, normalizes all Unicode characters 
(recursively) using NFKC normalization, and saves the normalized data into a new JSON file.

Environment Variables:
- BASE_DIR: Base directory for all files (from .env file)

Input:
- JSON file with vector data to normalize (input file path constructed dynamically).

Output:
- JSON file with normalized vector data (output file path constructed dynamically).
"""

import json
import os
import unicodedata
from utils import load_env

# Load environment variables using the helper function
load_env()
# Load the base directory
BASE_DIR = os.getenv('BASE_DIR')

def normalize_unicode_characters(data):
    """
    Recursively normalizes all strings in a nested data structure (dict/list).

    Parameters:
    - data: The input data (could be dict, list, or str).

    Returns:
    - The normalized data with all string values normalized using NFKC.
    """
    if isinstance(data, str):
        return unicodedata.normalize('NFKC', data)
    elif isinstance(data, list):
        return [normalize_unicode_characters(item) for item in data]
    elif isinstance(data, dict):
        return {key: normalize_unicode_characters(value) for key, value in data.items()}
    else:
        return data

def main():
    """
    Main function to read the vector data, normalize Unicode characters, and save the normalized data.
    """
    # Construct file paths using BASE_DIR
    input_file_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/01_converted_vectordata.json')
    output_file_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/02_normalized_vectordata.json')

    try:
        # Load the converted vector data
        with open(input_file_path, 'r', encoding='utf-8') as file:
            vector_database = json.load(file)

        # Normalize all Unicode characters in the entire data structure
        normalized_vector_database = normalize_unicode_characters(vector_database)

        # Save the normalized data to a new file
        with open(output_file_path, 'w', encoding='utf-8') as file:
            json.dump(normalized_vector_database, file, indent=4, ensure_ascii=False)

        print(f"Normalized vector data successfully saved to '{output_file_path}'.")
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
