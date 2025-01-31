"""
Script for translating Zoho Desk data using OpenAI's GPT-4 model and DSPy.

File Manipulation Details:
- Load input JSON data from a local file.
- Translate both the 'title' and 'answer' fields using the OpenAI GPT-4 model.
- Normalize Unicode characters in the data.
- Save the translated JSON data to a new output file.

Environment Variables Required:
- BASE_DIR: Base directory for project files.
- OPENAI_API_KEY: API key for OpenAI GPT-4 model.
- S3_BUCKET_NAME: Name of the S3 bucket (optional).
"""

import json
import os
import logging
import dspy
import unicodedata
from utils import load_env

# Load environment variables using the helper function
load_env()

# Use BASE_DIR from environment variables
BASE_DIR = os.getenv('BASE_DIR')

# Set up basic logging configuration (for GitHub Actions, console-only logging)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def normalize_unicode_characters(data):
    """
    Recursive function to normalize all strings in a nested object (dict or list).
    
    Args:
        data: The data structure (dict or list) to be normalized.
    
    Returns:
        The data structure with normalized strings.
    """
    if isinstance(data, str):
        return unicodedata.normalize('NFKC', data)
    elif isinstance(data, list):
        return [normalize_unicode_characters(item) for item in data]
    elif isinstance(data, dict):
        return {key: normalize_unicode_characters(value) for key, value in data.items()}
    else:
        return data

# Configure DSPy for translation using OpenAI GPT-4 model
dspy.configure(lm=dspy.OpenAI(model='gpt-4', api_key=os.getenv('OPENAI_API_KEY')))

class TranslateTitle(dspy.Signature):
    """Translate title field to US English."""
    content = dspy.InputField(descr="Title to be translated")
    translated_title = dspy.OutputField(descr="Translated title")

class TranslateAnswer(dspy.Signature):
    """Translate answer field to US English."""
    content = dspy.InputField(descr="Answer to be translated")
    translated_answer = dspy.OutputField(descr="Translated answer")

# Translation models for title and answer fields
title_translation_model = dspy.Predict(TranslateTitle)
answer_translation_model = dspy.Predict(TranslateAnswer)

def load_json(file_path):
    """
    Load data from a JSON file.
    
    Args:
        file_path (str): Path to the JSON file.
    
    Returns:
        data (list): Loaded JSON data as a list of dictionaries.
    """
    try:
        logging.info(f"Loading JSON file from {file_path}")
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logging.info(f"Successfully loaded {len(data)} records from JSON.")
        return data
    except Exception as e:
        logging.error(f"Error loading JSON file: {e}")
        return []

def save_json(data, file_path):
    """
    Save data to a JSON file.
    
    Args:
        data (list): The data to save.
        file_path (str): Path to the output JSON file.
    """
    try:
        logging.info(f"Saving translated data to {file_path}")
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        logging.info(f"Data saved successfully to {file_path}")
    except Exception as e:
        logging.error(f"Error saving JSON file: {e}")

def translate_content(old_content):
    """
    Translate title and answer fields using the GPT-4 model via DSPy.
    
    Args:
        old_content (dict): The original content with 'title' and 'answer'.
    
    Returns:
        dict: A dictionary containing translated 'title' and 'answer'.
    """
    normalized_content = normalize_unicode_characters(old_content)

    # Translate title
    try:
        title_translation_response = title_translation_model(content=normalized_content['title'])
        translated_title = title_translation_response.translated_title if title_translation_response else None
    except Exception as e:
        logging.error(f"Error translating title: {e}")
        translated_title = None

    # Translate answer
    try:
        answer_translation_response = answer_translation_model(content=normalized_content['answer'])
        translated_answer = answer_translation_response.translated_answer if answer_translation_response else None
    except Exception as e:
        logging.error(f"Error translating answer: {e}")
        translated_answer = None

    if translated_title and translated_answer:
        return {
            "title": translated_title,
            "answer": translated_answer
        }
    else:
        logging.warning("Translation failed for either title or answer.")
        return None

def process_json(input_file, output_file):
    """
    Main function to load, translate, and save JSON content.
    
    Args:
        input_file (str): Path to the input JSON file.
        output_file (str): Path to the output translated JSON file.
    """
    logging.info(f"Processing JSON file: {input_file}")
    
    data = load_json(input_file)
    translated_data = []

    for item in data:
        translated_item = translate_content(item)
        if translated_item:
            translated_data.append(translated_item)

    if translated_data:
        save_json(translated_data, output_file)
    else:
        logging.error("No translated data to save.")

# Define input and output file paths using BASE_DIR
input_file_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/zohlar-core_English.json')
output_file_path = os.path.join(BASE_DIR, 'zoho_desk_sync/initial_sync/data/zohlar-core_English_translated.json')

# Run the translation process
process_json(input_file_path, output_file_path)
