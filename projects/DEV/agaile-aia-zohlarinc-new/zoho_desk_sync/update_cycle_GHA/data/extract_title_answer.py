import json

def extract_title_and_answer(input_file_path, output_file_path):
    try:
        # Read the input JSON file
        with open(input_file_path, 'r', encoding='utf-8') as infile:
            data = json.load(infile)
        
        # Extract title and answer from each dataset
        extracted_data = [{'title': item.get('title'), 'answer': item.get('answer')} for item in data]
        
        # Write the extracted data to a new JSON file
        with open(output_file_path, 'w', encoding='utf-8') as outfile:
            json.dump(extracted_data, outfile, indent=4, ensure_ascii=False)
        
        print(f"Extracted data saved to {output_file_path}")
    
    except Exception as e:
        print(f"An error occurred: {e}")

# Define the input and output file paths
input_file_path = '/Users/joachimkohl/dev/ingestion-projects/projects/DEV/agaile-aia-zohlarinc-new/zoho_desk_sync/update_cycle_GHA/data/zz01_ZD_relevant_ids_and_variables.json'
output_file_path = '/Users/joachimkohl/dev/ingestion-projects/projects/DEV/agaile-aia-zohlarinc-new/zoho_desk_sync/update_cycle_GHA/data/faqs_title_answer.json'

# Call the function to extract data
extract_title_and_answer(input_file_path, output_file_path)