import csv
import requests
import logging
import os
import pandas as pd
import unicodedata
from openpyxl import Workbook
import random
from tqdm import tqdm
import datetime
import sys
from pathlib import Path

# Dynamically add the parent directory to sys.path
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parents[0]

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import and load environment variables using the helper function
from utils import load_env
load_env()

BASE_DIR = Path(os.getenv('BASE_DIR'))

# Construct paths using Path
INPUT_EXCEL_PATH = BASE_DIR / os.getenv('RAGAS_INPUT_EXCEL_PATH')
OUTPUT_CSV_PATH = BASE_DIR / os.getenv('RAGAS_OUTPUT_CSV_PATH')
EVALUATION_RESULTS_PATH = BASE_DIR / os.getenv('RAGAS_EVALUATION_RESULTS_PATH')
# Extract the original filename without extension
original_filename = Path(os.getenv('RAGAS_INPUT_EXCEL_PATH')).stem
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Normalize Unicode characters
def normalize_unicode_characters(data):
    if isinstance(data, str):
        return unicodedata.normalize('NFKC', data)
    elif isinstance(data, list):
        return [normalize_unicode_characters(item) for item in data]
    elif isinstance(data, dict):
        return {key: normalize_unicode_characters(value) for key, value in data.items()}
    else:
        return data

# Function to create a new chat
def create_new_chat(user_id):
    try:
        chat_id = f"CHAT_ID_RAGAS_be_{random.randint(1, 9999999)}"
        base_url = os.getenv('BASE_URL').rstrip('/')
        url = f"{base_url}/new_chat"
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "x-api-key": os.getenv('X_API_KEY')
        }
        response = requests.post(url, json={"chatID": chat_id, "userID": user_id}, headers=headers)
        logging.debug(f"Request to {url}: {{'chatID': {chat_id}, 'userID': {user_id}}}")
        logging.debug(f"Response: {response.status_code} - {response.text}")
        if response.status_code == 201 and response.json().get("chatID") == chat_id:
            logging.info("Neuer Chat erfolgreich erstellt.")
            return chat_id, user_id
        else:
            logging.error(f"Invalid chatID in response: {response.json()}")
            return None, None
    except Exception as e:
        logging.error(f"Fehler bei der Netzwerkanfrage für /new_chat: {e}")
        return None, None

# Function to get answers from the backend
def get_answer_from_backend(question, chat_id, user_id):
    try:
        base_url = os.getenv('BASE_URL').rstrip('/')
        url = f"{base_url}/chat_answer"
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "x-api-key": os.getenv('X_API_KEY')
        }
        payload = {
            "chatID": chat_id,
            "userID": user_id,
            "assistant": os.getenv('ASSISTANT'),
            "message_content": question,
            "streaming": False,
            "show_links": True
        }
        response = requests.post(url, json=payload, headers=headers)
        logging.debug(f"Serverantwort erhalten: Statuscode={response.status_code}, Inhalt={response.text[:500]}")
        if response.status_code == 200:
            try:
                response_json = response.json()
                message = response_json.get('message', '')
                links = ','.join(response_json.get('links', ['NA']))
                logging.info("Antwort vom Backend erfolgreich erhalten.")
                return message, links
            except json.JSONDecodeError:
                logging.error("Fehler beim Parsen der JSON-Antwort. Antworttext: " + response.text)
                return '', 'NA'
        else:
            logging.error(f"Fehler beim Erhalten der Antwort für /chat_answer: HTTP-Status {response.status_code}, Antworttext: {response.text}")
            return '', 'NA'
    except Exception as e:
        logging.error(f"Fehler bei der Netzwerkanfrage für /chat_answer: {e}")
        return '', 'NA'

# Function to read CSV data
def read_csv(file_path):
    try:
        data = []
        with open(file_path, encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                if row['question'] and row['ground_truths']:
                    data.append((row['question'], row['ground_truths']))
        return data
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return []

# Function to save results to CSV
def save_results_csv(questions, ground_truths, answers, answer_links, file_path):
    try:
        fieldnames = ['question', 'ground_truths', 'answer', 'answer_links']
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for q, g, a, l in zip(questions, ground_truths, answers, answer_links):
                writer.writerow({'question': q, 'ground_truths': g, 'answer': a, 'answer_links': l})
        logging.info(f"Results saved to {file_path}")
    except Exception as e:
        logging.error(f"Error saving results: {e}")

# Function to convert Excel to CSV
def convert_xlsx_to_csv(xlsx_file_path, csv_file_path):
    try:
        df = pd.read_excel(xlsx_file_path)
        df.to_csv(csv_file_path, index=False, encoding='utf-8')
        logging.info(f"Converted {xlsx_file_path} to {csv_file_path}")
    except Exception as e:
        logging.error(f"Error converting XLSX to CSV: {e}")

# Function to convert CSV to Excel
def convert_csv_to_xlsx(csv_file_path, xlsx_file_path):
    try:
        df = pd.read_csv(csv_file_path, encoding='utf-8')
        df.to_excel(xlsx_file_path, index=False)
        logging.info(f"Converted {csv_file_path} to {xlsx_file_path}")
    except Exception as e:
        logging.error(f"Error converting CSV to XLSX: {e}")

# Main function to process CSV and save results to XLSX
def process_csv_and_save_to_xlsx(input_excel_path, output_csv_path, evaluation_results_path):
    try:
        # Convert Excel to CSV
        convert_xlsx_to_csv(input_excel_path, output_csv_path)

        # Read data from the newly created CSV
        data = read_csv(output_csv_path)
        questions, ground_truths = zip(*data)
        answers, answer_links = [], []

        for question in tqdm(questions, desc='Processing questions'):
            user_id = f"USER_ID_RAGAS_be_{random.randint(1, 9999999)}"
            chat_id, user_id = create_new_chat(user_id)
            if chat_id:
                answer, links = get_answer_from_backend(question, chat_id, user_id)
                answers.append(normalize_unicode_characters(answer))
                answer_links.append(normalize_unicode_characters(links))
            else:
                answers.append('')
                answer_links.append('NA')

        save_results_csv(questions, ground_truths, answers, answer_links, output_csv_path)

        now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        final_xlsx_path = evaluation_results_path.parent / f"{now}_evaluation_{original_filename}.xlsx"
        convert_csv_to_xlsx(output_csv_path, final_xlsx_path)
    except Exception as e:
        logging.error(f"Error: {e}")

if __name__ == "__main__":
    if not INPUT_EXCEL_PATH.exists() or not OUTPUT_CSV_PATH or not EVALUATION_RESULTS_PATH:
        logging.error("Missing or incorrect environment variables.")
    else:
        process_csv_and_save_to_xlsx(INPUT_EXCEL_PATH, OUTPUT_CSV_PATH, EVALUATION_RESULTS_PATH)