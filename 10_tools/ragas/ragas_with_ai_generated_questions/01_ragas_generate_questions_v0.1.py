#!/usr/bin/env python

print("Script starting...")  # Debug print

import dspy
import pandas as pd
import json
from tqdm import tqdm
import sys
import os
import logging
import random
from dotenv import load_dotenv
from pathlib import Path

print("Imports completed")  # Debug print

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Configure logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Print to console
        logging.FileHandler('question_generation.log')  # Also save to file
    ]
)

print("Logging configured")  # Debug print

# Dynamically add the parent directory to sys.path
script_dir = Path(__file__).resolve().parent
parent_dir = script_dir.parent.parent
sys.path.append(str(parent_dir))

print(f"Script directory: {script_dir}")  # Debug print
print(f"Parent directory: {parent_dir}")  # Debug print

try:
    # Now import utils after sys.path is set
    from utils import load_env
    print("Utils imported successfully")  # Debug print
    
    # Load environment variables using the helper function
    load_env()
    print("Environment variables loaded")  # Debug print
    
except Exception as e:
    print(f"Error during utils import or env loading: {e}")
    sys.exit(1)

# Check required environment variables
required_env_vars = [
    'BASE_DIR',
    'OPENAI_API_KEY',
    'USE_EXCEL',
    'SAMPLE_SIZE'
]

missing_vars = [var for var in required_env_vars if os.getenv(var) is None]

if missing_vars:
    logging.error(f"Missing environment variables: {', '.join(missing_vars)}")
    logging.error("Please ensure all required environment variables are set in your .env file")
    sys.exit(1)

print("Environment variables checked")  # Debug print

# Load necessary variables
BASE_DIR = os.getenv('BASE_DIR')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
USE_EXCEL = os.getenv('USE_EXCEL', 'true').lower() == 'true'

print(f"BASE_DIR: {BASE_DIR}")  # Debug print
print(f"OPENAI_API_KEY exists: {'Yes' if OPENAI_API_KEY else 'No'}")  # Debug print
print(f"USE_EXCEL: {USE_EXCEL}")  # Debug print

print("Configuring DSPy...")  # Debug print
dspy.configure(lm=dspy.OpenAI(model='gpt-4', api_key=OPENAI_API_KEY))
print("DSPy configured")  # Debug print

class GenerateQuestion(dspy.Signature):
    """
    Your task is to formulate a concise question using a maximum of 5 words. 
    Avoid technical terms and use simple, universally understandable words. 
    Keep the question brief and focused on the main content rather than specific details. 
    Generate only one question per input.
    The question should be clear and natural-sounding.
    """
    content = dspy.InputField(desc="Base content for question generation")
    question = dspy.OutputField(desc="Generated question")

generate_question_model = dspy.ChainOfThought(GenerateQuestion)

def generate_question(content):
    response = generate_question_model(content=content)
    return response.question.strip()

def process_json_file(input_file, output_file, sample_size, use_excel: bool = True):
    logging.info(f"Processing input file: {input_file}")
    
    if not os.path.exists(input_file):
        logging.error(f"Input file not found: {input_file}")
        return False
        
    with open(input_file, 'r') as file:
        data = json.load(file)

    logging.info(f"Loaded {len(data)} items from JSON file")

    sample_size = int(sample_size)
    sample_size = min(sample_size, len(data))
    random.seed()
    data = random.sample(data, sample_size)

    pbar = tqdm(total=len(data), desc="Generating questions", unit="row")
    result_df = pd.DataFrame(columns=['question', 'ground_truth'])

    for item in data:
       # content = item['content'] use another content item name based on the source file
        content = item['answer']
        question = generate_question(content)
        new_row = pd.DataFrame({'question': [question], 'ground_truth': [content]})
        result_df = pd.concat([result_df, new_row], ignore_index=True)
        pbar.update(1)

    if use_excel:
        result_df.to_excel(output_file, index=False)
    else:
        result_df.to_csv(output_file, index=False, encoding='utf-8')
    pbar.close()
    logging.info(f"File {os.path.basename(input_file)} successfully processed and saved to {output_file}")
    return True

def main():
    print("Starting main function...")  # Debug print
    logging.info("Starting question generation process...")
    
    sample_size_input = os.getenv('SAMPLE_SIZE', '0')
    print(f"Sample size input: {sample_size_input}")  # Debug print

    if sample_size_input.isdigit():
        sample_size = int(sample_size_input)
    elif sample_size_input.lower() == 'all':
        sample_size = 0
    else:
        logging.error("Invalid input for sample_size. Please enter a number or 'all'.")
        sys.exit(1)

    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, 'data')
    os.makedirs(data_dir, exist_ok=True)
    # Input and output paths
    input_file = os.path.join(data_dir, 'latest_vectordata_zoho_sync.json')
    output_file = os.path.join(data_dir, f"1_test_data_set_questions_ground_truths.{'xlsx' if USE_EXCEL else 'csv'}")

    print(f"Input file path: {input_file}")  # Debug print
    print(f"Output file path: {output_file}")  # Debug print

    success = process_json_file(input_file, output_file, sample_size, USE_EXCEL)
    
    if success:
        print("Question generation completed successfully")  # Debug print
    else:
        print("Question generation failed")  # Debug print
        sys.exit(1)

if __name__ == "__main__":
    print("Starting script execution...")  # Debug print
    main()
    print("Script completed")  # Debug print