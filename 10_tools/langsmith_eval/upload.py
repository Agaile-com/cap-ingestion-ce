import os
import json
from apify_client import ApifyClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve variables from environment
API_TOKEN = os.getenv('API_TOKEN')
DATASET_NAME = os.getenv('DATASET_NAME')
OUTPUT_FILE = os.getenv('OUTPUT_FILE')  # Updated to use OUTPUT_FILE

# Initialize the Apify client
client = ApifyClient(API_TOKEN)

def upload_dataset():
    # Validate that the OUTPUT_FILE exists
    if not os.path.exists(OUTPUT_FILE):
        raise FileNotFoundError(f"The file '{OUTPUT_FILE}' does not exist.")
    
    # Load the cleaned JSON data
    with open(OUTPUT_FILE, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Retrieve or create the dataset with the specified name
    print(f"Retrieving or creating dataset: {DATASET_NAME}")
    dataset = client.datasets().get_or_create(name=DATASET_NAME)
    dataset_id = dataset['id']
    print(f"Dataset '{DATASET_NAME}' is ready with ID: {dataset_id}")

    # Upload the data to the dataset
    print("Uploading data...")
    client.dataset(dataset_id).push_items(data)
    print(f"Data successfully uploaded to dataset '{DATASET_NAME}'.")

if __name__ == "__main__":
    try:
        upload_dataset()
    except Exception as e:
        print(f"An error occurred: {e}")