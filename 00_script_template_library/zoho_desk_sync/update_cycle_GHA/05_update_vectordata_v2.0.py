"""
File Manipulation Docstring:

This script is responsible for processing and updating vector data stored in AWS S3 by applying a series of transformations and merging datasets. 

- **Load JSON from S3**:
  - Bucket: Retrieved from the environment variable `S3_BUCKET_NAME`.
  - Key: Defined for each dataset (e.g., `<TENANT_NAME>/zohodesk-data/enriched/02_synced_vectordata_with_keywords_only.json`).
  - Manipulation: Normalizes Unicode characters in the loaded data.

- **Save JSON to S3**:
  - Bucket: Same as the load bucket.
  - Key: Modifies the input key to include `_processed` before `.json` (e.g., `<TENANT_NAME>/zohodesk-data/enriched/02_synced_vectordata_with_keywords_only_processed.json`).
  - Manipulation: Normalizes Unicode characters in data before saving.

- **Process File**:
  - Sequentially applies provided processing functions (e.g., `enrich_with_keywords`, `generate_combined_text`).
  - Each function reads from an input key, processes the data, and writes to an intermediate or final output key.
  - Temporary keys are used during intermediate steps to prevent overwriting.

- **Merge Datasets**:
  - Keys: Collects all processed keys and an additional predefined key (`<TENANT_NAME>/zohodesk-data/enriched/01_synced_vectordata_with_both.json`).
  - Final Key: `<TENANT_NAME>/zohodesk-data/synced/vectordata_YYYY-MM-DD_HH-MM-SS.json`.
  - Manipulation: Merges all datasets into a single JSON array and saves the merged data to S3.

Steps:
  1. Define input and output keys for each dataset.
  2. Apply processing functions to each dataset.
  3. Save processed data to S3.
  4. Merge all processed datasets along with an additional dataset into one.
  5. Save the merged dataset to S3.
"""

import os
import json
import logging
import boto3

import unicodedata
from datetime import datetime, timezone
import sys
from pathlib import Path
from utils import load_env

# Load environment variables using the helper function
load_env()
# Get base directory of the script
base_dir = Path(__file__).resolve().parent

# Pfad für zusätzliche Bibliotheken (custom functions)
lib_path = (base_dir / "lib").resolve()
sys.path.append(str(lib_path))

# Import custom functions
from a_enrich_with_keywords_dspy import enrich_with_keywords
from b_generate_combined_text import enrich_with_combined_text

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# AWS S3 Configuration
s3_client = boto3.client('s3')
bucket_name = os.getenv('S3_BUCKET_NAME')
storage_prefix = f"{os.getenv('TENANT_NAME')}/zohodesk-data"

def normalize_unicode_characters(data):
    """
    Recursively normalizes Unicode characters in a nested structure (dictionary or list).

    Args:
        data: The structure (str, list, dict) to be normalized.

    Returns:
        The normalized structure with Unicode characters handled using NFKC normalization.
    """
    if isinstance(data, str):
        return unicodedata.normalize('NFKC', data)
    elif isinstance(data, list):
        return [normalize_unicode_characters(item) for item in data]
    elif isinstance(data, dict):
        return {key: normalize_unicode_characters(value) for key, value in data.items()}
    else:
        return data

def load_json_from_s3(bucket, key):
    """
    Loads JSON data from a specified S3 bucket and key.

    Args:
        bucket (str): The name of the S3 bucket.
        key (str): The key (path) of the file to be loaded from S3.

    Returns:
        dict or list: The loaded and normalized JSON data.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.load(response['Body'])
        normalized_data = normalize_unicode_characters(data)
        logging.info(f"Data successfully loaded from {key}")
        return normalized_data
    except Exception as e:
        logging.error(f"Error loading data from {key}: {e}")
        return None

def save_json_to_s3(bucket, data, key):
    """
    Saves JSON data to a specified S3 bucket and key.

    Args:
        bucket (str): The name of the S3 bucket.
        data (dict or list): The data to be saved in JSON format.
        key (str): The S3 key (path) where the data will be stored.
    """
    try:
        normalized_data = normalize_unicode_characters(data)
        s3_client.put_object(Bucket=bucket, Body=json.dumps(normalized_data, indent=4), Key=key)
        logging.info(f"Data successfully saved to {key}")
    except Exception as e:
        logging.error(f"Error saving data to {key}: {e}")
        exit(1)

def process_file(bucket, input_key, output_key, processing_functions):
    """
    Sequentially applies processing functions to a file loaded from S3, saving the result back to S3.

    Args:
        bucket (str): The name of the S3 bucket.
        input_key (str): The input S3 key.
        output_key (str): The output S3 key.
        processing_functions (list): A list of functions to be applied in sequence.
    """
    intermediate_key = input_key  # Start with the input key
    for function in processing_functions:
        # Temporary output key for intermediate steps
        intermediate_output_key = intermediate_key.replace('.json', '_temp.json') if function != processing_functions[-1] else output_key
        function(bucket, intermediate_key, intermediate_output_key)
        intermediate_key = intermediate_output_key  # The output of this function becomes the input for the next function

def merge_datasets(bucket, keys, final_key):
    """
    Merges multiple datasets from S3 and saves the merged result back to S3.

    Args:
        bucket (str): The name of the S3 bucket.
        keys (list): A list of S3 keys containing the datasets to be merged.
        final_key (str): The S3 key where the merged dataset will be saved.
    """
    merged_data = []
    for key in keys:
        data = load_json_from_s3(bucket, key)
        if data:
            merged_data.extend(data)
    save_json_to_s3(bucket, merged_data, final_key)

def main():
    """
    Main function to process vector data files from S3 and merge the processed results.

    - Loads data from specified input keys.
    - Applies transformation functions to enrich the data.
    - Merges the processed datasets into a single output.
    - Saves the final result to S3.
    """
    enriched_prefix = f"{storage_prefix}/enriched/"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")  # Generate UTC timestamp for versioning
    final_key = f"{storage_prefix}/synced/vectordata_{timestamp}.json"  # Path for the final output with timestamp

    # Define input and output paths and corresponding processing functions
    paths = {
        'with_keywords_only': ('02_synced_vectordata_with_keywords_only.json', [enrich_with_combined_text]),
        'with_combined_only': ('03_synced_vectordata_with_combined_only.json', [enrich_with_keywords]),
        'without_both': ('04_synced_vectordata_without_both.json', [enrich_with_keywords, enrich_with_combined_text])
    }

    # Process each file with the associated functions
    for path_key, (input_file, functions) in paths.items():
        input_key = f'{enriched_prefix}{input_file}'
        output_key = input_key.replace('.json', '_processed.json')  # Avoid overwriting the input file
        process_file(bucket_name, input_key, output_key, functions)

    # Additional file that was previously processed
    with_both_key = f'{enriched_prefix}01_synced_vectordata_with_both.json'

    # Collect all processed keys
    processed_keys = [f'{enriched_prefix}{file.replace(".json", "_processed.json")}' for file, _ in paths.values()]
    processed_keys.append(with_both_key)  # Add the pre-existing file to the merge

    # Merge all processed datasets
    merge_datasets(bucket_name, processed_keys, final_key)

    logging.info('All datasets have been successfully merged and saved to S3.')

if __name__ == "__main__":
    main()
