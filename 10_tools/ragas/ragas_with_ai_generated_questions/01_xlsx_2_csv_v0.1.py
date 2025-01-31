from xlsx2csv import Xlsx2csv
import logging
import os
from dotenv import load_dotenv
import sys

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def convert_excel_to_csv(excel_path, csv_path):
    try:
        # Convert Excel file to CSV
        logging.info(f'Reading Excel file from {excel_path}')
        Xlsx2csv(excel_path, outputencoding="utf-8").convert(csv_path)
        logging.info(f'Data saved to CSV file {csv_path}')
        logging.info('Conversion successful.')

    except Exception as e:
        logging.error(f'Error during conversion: {e}')
        sys.exit(1)

# Load paths from .env file
excel_path = os.getenv('EXCEL_PATH')
csv_path = os.getenv('CSV_PATH')

# Execute conversion
if not excel_path or not csv_path:
    logging.error('Excel path or CSV path is not defined. Ensure the .env file is correctly configured.')
    sys.exit(1)

convert_excel_to_csv(excel_path, csv_path)