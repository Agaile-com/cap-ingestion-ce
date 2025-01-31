import os
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

INPUT_FOLDER = os.getenv("INPUT_FOLDER")
OUTPUT_FILE = os.getenv("OUTPUT_FILE")

# Define the condition to remove datasets with the specific markdown content
def condition_markdown_page_not_found(dataset):
    target_markdown = (
        "Page Not Found\n\n#### Sign up now\n\nFull Name \\* \n\nLast Name \\* \n\nWorking mail \\* \n\n"
        "Password \\* \n\nPassword \\* \n\nSign Up\n\n#### Welcome back\n\nEmail Address \\* \n\nPassword \\* \n\n"
        "Sign In\n\n404\n\n# Page not found\n\nThe page you are looking for might have been removed, "
        "had its name changed, or is temporarily unavailable.\n\n[Back to home](/)"
    )
    return dataset.get("markdown", "").strip() == target_markdown

# Function to clean the JSON datasets
def clean_json_datasets(input_folder, output_file, conditions):
    cleaned_datasets = []
    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            input_file_path = os.path.join(input_folder, filename)
            print(f"Processing file: {input_file_path}")
            with open(input_file_path, "r", encoding="utf-8") as file:
                try:
                    data = json.load(file)
                    if isinstance(data, list):
                        # Filter out datasets that meet any condition
                        filtered_data = [
                            dataset for dataset in data
                            if not any(condition(dataset) for condition in conditions)
                        ]
                        cleaned_datasets.extend(filtered_data)
                    else:
                        print(f"Skipped file {filename}: Not a list of datasets")
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON in file {filename}: {e}")
    
    # Save cleaned datasets to the output file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as output_file_obj:
        json.dump(cleaned_datasets, output_file_obj, ensure_ascii=False, indent=4)
    print(f"Cleaned datasets saved to {output_file}")

# Main script
if __name__ == "__main__":
    conditions = [condition_markdown_page_not_found]
    clean_json_datasets(INPUT_FOLDER, OUTPUT_FILE, conditions)