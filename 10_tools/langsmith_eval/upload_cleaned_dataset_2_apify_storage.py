import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get variables from the .env file
API_TOKEN = os.getenv('API_TOKEN')
DATASET_NAME = os.getenv('DATASET_NAME')
JSON_FILE_PATH = os.getenv('JSON_FILE_PATH')

# Base URL for Apify API
BASE_URL = 'https://api.apify.com/v2/datasets'

# Create a new dataset (if not already created)
def create_dataset():
    url = f'{BASE_URL}?token={API_TOKEN}'
    response = requests.post(url, json={"name": DATASET_NAME})
    if response.status_code == 201:
        print(f"Dataset '{DATASET_NAME}' created successfully.")
        return response.json()['id']
    elif response.status_code == 409:
        print(f"Dataset '{DATASET_NAME}' already exists.")
        return DATASET_NAME  # Use the same dataset name if it exists
    else:
        raise Exception(f"Failed to create dataset: {response.text}")

# Upload the edited data to the dataset
def upload_data(dataset_id, data):
    url = f'{BASE_URL}/{dataset_id}/items?token={API_TOKEN}'
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 201:
        print(f"Data uploaded successfully to dataset '{DATASET_NAME}'.")
    else:
        raise Exception(f"Failed to upload data: {response.text}")

# Main script
if __name__ == '__main__':
    try:
        # Load your edited JSON data
        with open(JSON_FILE_PATH, 'r') as file:
            data = json.load(file)
        
        # Create the dataset and get its ID
        dataset_id = create_dataset()

        # Upload data to the dataset
        upload_data(dataset_id, data)
    except Exception as e:
        print(f"An error occurred: {e}")