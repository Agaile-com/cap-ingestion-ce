import os
import logging
import psycopg2
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define possible paths for the .env file
POSSIBLE_ENV_PATHS = [
    Path(__file__).resolve().parent.parent / '.env',
    Path(__file__).resolve().parent / '.env',
    Path.home() / '.env'
]

def load_env_file():
    for env_path in POSSIBLE_ENV_PATHS:
        if env_path.exists():
            print(f".env file found and loaded from: {env_path}")
            load_dotenv(env_path)
            return True
    print("Warning: No .env file found in the following locations:")
    for path in POSSIBLE_ENV_PATHS:
        print(f"  - {path}")
    print("Please create a .env file in one of these locations or set environment variables manually.")
    return False

# Try to load the .env file
load_env_file()

print(f"Loaded .env file: {os.getenv('POSTGRESQL_DB_PASSWORD')[:3]}***")

# Database configuration
DB_HOST = os.getenv('POSTGRESQL_DB_HOST')
DB_PORT = os.getenv('POSTGRESQL_DB_PORT', '5432')
DB_NAME = os.getenv('POSTGRESQL_DB_NAME')
DB_USER = os.getenv('POSTGRESQL_DB_USER')
DB_PASSWORD = os.getenv('POSTGRESQL_DB_PASSWORD')

def print_env_variables():
    logger.info("Environment Variables:")
    logger.info(f"POSTGRESQL_DB_HOST: {DB_HOST}")
    logger.info(f"POSTGRESQL_DB_PORT: {DB_PORT}")
    logger.info(f"POSTGRESQL_DB_NAME: {DB_NAME}")
    logger.info(f"POSTGRESQL_DB_USER: {DB_USER}")
    logger.info(f"POSTGRESQL_DB_PASSWORD: {'[SET]' if DB_PASSWORD else '[NOT SET]'}")

def connect_to_postgres():
    try:
        connection_params = {
            "host": DB_HOST,
            "port": DB_PORT,
            "database": DB_NAME,
            "user": DB_USER,
            "password": DB_PASSWORD
        }
        logger.info(f"Attempting to connect with: host={DB_HOST}, port={DB_PORT}, database={DB_NAME}, user={DB_USER}")
        
        # Try to establish a connection
        conn = psycopg2.connect(**connection_params)
        
        # If successful, create and return the SQLAlchemy engine
        engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        logger.info("Successfully created database engine")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to the database: {str(e)}")
        raise

def get_record_count():
    try:
        engine = connect_to_postgres()
        with engine.connect() as connection:
            result = connection.execute(text("SELECT COUNT(*) FROM public.langchain_pg_embedding"))
            count = result.scalar()
            logger.info(f"Number of records in langchain_pg_embedding: {count}")
            return count
    except ValueError as ve:
        logger.error(f"Environment Error: {ve}")
    except Exception as e:
        logger.error(f"Error querying the database: {e}")
    return None

if __name__ == "__main__":
    print_env_variables()
    record_count = get_record_count()
    if record_count is not None:
        print(f"Total number of records: {record_count}")
    else:
        print("Failed to retrieve record count.")
    
    if not any([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        print("\nIt seems that your environment variables are not set. Here's how to set them:")
        print("1. Create a .env file in one of the following locations:")
        for path in POSSIBLE_ENV_PATHS:
            print(f"   - {path}")
        print("2. Add the following lines to your .env file (replace with your actual values):")
        print("   POSTGRESQL_DB_HOST=your_host")
        print("   POSTGRESQL_DB_PORT=your_port")
        print("   POSTGRESQL_DB_NAME=your_db_name")
        print("   POSTGRESQL_DB_USER=your_username")
        print("   POSTGRESQL_DB_PASSWORD=your_password")
        print("3. Run this script again.")
        print("\nAlternatively, you can set these as environment variables in your shell before running the script.")