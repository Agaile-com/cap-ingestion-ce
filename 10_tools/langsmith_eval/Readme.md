Data Cleaning and Upload to Apify Storage

This project automates the preprocessing, cleaning, and uploading of datasets to Apify storage, ensuring consistent data quality and readiness for further processing. The workflow supports flexible configuration through environment variables and handles error scenarios gracefully.

Features
	1.	Data Cleaning
	•	Removes unwanted datasets based on specific conditions (e.g., datasets containing specific values in the markdown field).
	•	Maintains the original JSON structure during preprocessing.
	2.	Automated Upload
	•	Uploads cleaned datasets to Apify storage.
	•	Creates or retrieves a dataset on Apify and pushes the cleaned data.
	3.	Environment Configuration
	•	Utilizes a .env file to manage file paths, API tokens, and dataset names for a seamless workflow.

Setup

1. Prerequisites
	•	Python 3.12 or higher
	•	Required Python packages:
	•	apify-client
	•	python-dotenv

2. Clone the Repository

git clone https://github.com/your-repository-name.git
cd your-repository-name

3. Install Dependencies

pip install -r requirements.txt

4. Configure Environment Variables

Create a .env file in the project root directory and populate it with the following values:

# Apify API token
API_TOKEN=your_apify_api_token

# Apify dataset name for uploading cleaned data
DATASET_NAME=agile-ai-cleaned-dataset

# Path to the folder containing original datasets
INPUT_FOLDER=/path/to/original/json/folder

# Path to the cleaned dataset output file
OUTPUT_FILE=/path/to/cleaned/json/file/agile-ai-cleaned-dataset.json

Usage

1. Data Cleaning

The script processes all JSON files in the INPUT_FOLDER, removes datasets that match specified conditions, and writes the cleaned data to the OUTPUT_FILE.

Run the following command:

python3 edit_datasets.py

2. Upload Cleaned Dataset to Apify

After cleaning the dataset, upload it to Apify storage:

python3 upload_dataset.py

Code Overview

File: edit_datasets.py
	•	Purpose: Removes unwanted datasets based on specified conditions and saves the cleaned data to the output file.
	•	Key Features:
	•	Reads JSON files from the INPUT_FOLDER.
	•	Removes datasets containing specific text (e.g., “Page Not Found” in the markdown field).
	•	Saves the cleaned data to the OUTPUT_FILE.

File: upload_dataset.py
	•	Purpose: Uploads the cleaned dataset to Apify storage.
	•	Key Features:
	•	Retrieves or creates a dataset on Apify using the API token.
	•	Pushes cleaned data from the OUTPUT_FILE to the Apify dataset.

Error Handling
	•	Missing Files: The script checks for the existence of input files and raises an error if they are not found.
	•	Invalid JSON: The script validates JSON files before processing.
	•	API Errors: Proper error messages are displayed for failed API requests.

Example Workflow
	1.	Place the original JSON files in the INPUT_FOLDER.
	2.	Run edit_datasets.py to preprocess and clean the data.
	3.	Run upload_dataset.py to upload the cleaned data to Apify storage.

Dependencies
	•	apify-client: For interacting with Apify’s API.
	•	python-dotenv: For managing environment variables.
	•	os, json: Standard Python libraries for file and data manipulation.

Contributing

Feel free to contribute by creating pull requests or reporting issues in the repository. Make sure to follow the established code structure and style.

License

This project is licensed under the MIT License. See the LICENSE file for details.

Contact

For any questions or support, please contact:
	•	Name: Joachim Kohl
	•	Email: contact@agaile.ai