"""
File Manipulation Description:
- **Load JSON data from AWS S3**:
  - Bucket: Retrieved from environment variable `S3_BUCKET_NAME`.
  - Key: `<TENANT_NAME>/zohodesk-data/synced/vectordata_YYYY-MM-DD_HH-MM-SS.json`.
  - Data is loaded into memory and saved to a temporary JSON file on the local disk.

- **Process JSON data**:
  - Uses `JSONLoader` to load data from the temporary file.
  - Applies `metadata_func` to each record to update metadata fields.
  - Metadata fields include identifiers and descriptive data such as `title`, `link`, `keywords`, etc.

- **Delete temporary file**:
  - After processing, the temporary file is deleted from the local disk.

- **Embedding and Database Operations**:
  - Text data is embedded using `BedrockEmbeddings` with the model ID from `.env`.
  - Embeddings and metadata are uploaded to a PostgreSQL database using `PGVector`.
  - Connection to PostgreSQL is secured with AWS RDS IAM authentication.
  - Data in the specified table (`public.langchain_pg_embedding`) is deleted before new data is inserted.
"""
import os
import json
import boto3
import logging
import re
import tempfile
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from urllib.parse import quote_plus
from langchain_aws import BedrockEmbeddings  # Aktualisierter Import
from langchain_community.vectorstores import PGVector  # Aktualisierter Import
from langchain_community.document_loaders import JSONLoader
import sys
from pathlib import Path

# Dynamically add the parent directory (zoho_desk_sync) to sys.path
script_dir = Path(__file__).resolve().parent
parent_dir = script_dir.parent  # This will resolve to zoho_desk_sync
sys.path.append(str(parent_dir))

# Now import utils after sys.path is set
from utils import load_env
# Load environment variables using the helper function
load_env()


# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS S3 Configuration
s3_client = boto3.client('s3')
bucket_name = os.getenv('S3_BUCKET_NAME')
storage_prefix = f"{os.getenv('TENANT_NAME')}/zohodesk-data"

# PostgreSQL configuration
DB_NAME = os.getenv('POSTGRESQL_DB_NAME')
DB_USER = os.getenv('POSTGRESQL_DB_USER')# Admin-Benutzername, den du verwendest
DB_HOST = os.getenv('POSTGRESQL_DB_HOST')
DB_PORT = int(os.getenv('POSTGRESQL_DB_PORT'))
DB_PASSWORD = os.getenv('POSTGRESQL_DB_PASSWORD')
REGION = os.getenv('AWS_REGION')

# Bedrock Service Configuration from .env
BEDROCK_SERVICE_NAME = os.getenv('BEDROCK_SERVICE_NAME', 'bedrock-runtime')
BEDROCK_REGION_NAME = os.getenv('BEDROCK_REGION_NAME', 'eu-central-1')

# Titan embedding model configuration from .env
TITAN_MODEL_ID = os.getenv('TITAN_MODEL_ID', 'amazon.titan-embed-text-v2')

def metadata_func(record: dict, metadata: dict) -> dict:
    """
    Metadata update function for transforming Zoho article data to the desired format.
    
    This function references a previous implementation of `metadata_func` that 
    handles fields such as `id`, `title`, `keywords`, `meta_description`, and 
    several nested metadata fields including `zd_metadata`.
    
    Args:
        record (dict): Original record data from Zoho.
        metadata (dict): Metadata structure to be updated.

    Returns:
        dict: Updated metadata.
    """
    metadata.update({
                    "namespace": record.get("namespace", ""),
                    "id": record.get("id"),  
                    "title": record.get("title"),  
                    "answer": record.get("answer"),   
                    "link": record.get("link"),
                    "parent": record.get("parent"),
                    "keywords": record.get("keywords", []),
                    "meta_description": record.get("meta_description"),
                    "combined_text": record.get("combined_text"),
                    "category": record.get("metadata", {}).get("category", ""),
                    "sub_category": record.get("metadata", {}).get("sub_category", ""),
                    "tags": record.get("metadata", {}).get("tags", []),
                    "last_updated": record.get("metadata", {}).get("last_updated"),
                    "author": record.get("metadata", {}).get("author", ""),
                    "views": record.get("metadata", {}).get("views"),
                    "like": record.get("metadata", {}).get("like"),
                    "difficulty_level": record.get("metadata", {}).get("difficulty_level", ""),
                    "version": record.get("metadata", {}).get("version"),
                    "related_links": record.get("metadata", {}).get("related_links", []),
                    "zd_metadata": {
                        "modifiedTime": record.get("metadata", {}).get("zd_metadata", {}).get("modifiedTime"),
                        "departmentId": record.get("metadata", {}).get("zd_metadata", {}).get("departmentId"),
                        "creatorId": record.get("metadata", {}).get("zd_metadata", {}).get("creatorId"),
                        "dislikeCount": record.get("metadata", {}).get("zd_metadata", {}).get("dislikeCount"),
                        "modifierId": record.get("metadata", {}).get("zd_metadata", {}).get("modifierId"),
                        "likeCount": record.get("metadata", {}).get("zd_metadata", {}).get("likeCount"),
                        "locale": record.get("metadata", {}).get("zd_metadata", {}).get("locale"),
                        "ownerId": record.get("metadata", {}).get("zd_metadata", {}).get("ownerId"),
                        "translationState": record.get("metadata", {}).get("zd_metadata", {}).get("translationState"),
                        "isTrashed": record.get("metadata", {}).get("zd_metadata", {}).get("isTrashed"),
                        "createdTime": record.get("metadata", {}).get("zd_metadata", {}).get("createdTime"),
                        "modifiedBy": record.get("metadata", {}).get("zd_metadata", {}).get("modifiedBy"),
                        "id": record.get("metadata", {}).get("zd_metadata", {}).get("id"),
                        "viewCount": record.get("metadata", {}).get("zd_metadata", {}).get("viewCount"),
                        "translationSource": record.get("metadata", {}).get("zd_metadata", {}).get("translationSource"),
                        "owner": record.get("metadata", {}).get("zd_metadata", {}).get("owner"),
                        "summary": record.get("metadata", {}).get("zd_metadata", {}).get("summary"),
                        "latestVersionStatus": record.get("metadata", {}).get("zd_metadata", {}).get("latestVersionStatus"),
                        "author": record.get("metadata", {}).get("zd_metadata", {}).get("author"),
                        "permission": record.get("metadata", {}).get("zd_metadata", {}).get("permission"),
                        "authorId": record.get("metadata", {}).get("zd_metadata", {}).get("authorId"),
                        "usageCount": record.get("metadata", {}).get("zd_metadata", {}).get("usageCount"),
                        "commentCount": record.get("metadata", {}).get("zd_metadata", {}).get("commentCount"),
                        "rootCategoryId": record.get("metadata", {}).get("zd_metadata", {}).get("rootCategoryId"),
                        "sourceLocale": record.get("metadata", {}).get("zd_metadata", {}).get("sourceLocale"),
                        "translationId": record.get("metadata", {}).get("zd_metadata", {}).get("translationId"),
                        "createdBy": record.get("metadata", {}).get("zd_metadata", {}).get("createdBy"),
                        "latestVersion": record.get("metadata", {}).get("zd_metadata", {}).get("latestVersion"),
                        "webUrl": record.get("metadata", {}).get("zd_metadata", {}).get("webUrl"),
                        "feedbackCount": record.get("metadata", {}).get("zd_metadata", {}).get("feedbackCount"),
                        "portalUrl": record.get("metadata", {}).get("zd_metadata", {}).get("portalUrl"),
                        "attachmentCount": record.get("metadata", {}).get("zd_metadata", {}).get("attachmentCount"),
                        "latestPublishedVersion": record.get("metadata", {}).get("zd_metadata", {}).get("latestPublishedVersion"),
                        "position": record.get("metadata", {}).get("zd_metadata", {}).get("position"),
                        "availableLocaleTranslations": record.get("metadata", {}).get("zd_metadata", {}).get("availableLocaleTranslations", []),
                        "category": record.get("metadata", {}).get("zd_metadata", {}).get("category"),
                        "permalink": record.get("metadata", {}).get("zd_metadata", {}).get("permalink"),
                        "categoryId": record.get("metadata", {}).get("zd_metadata", {}).get("categoryId"),
                        "status": record.get("metadata", {}).get("zd_metadata", {}).get("status"),
                        "tags": record.get("metadata", {}).get("zd_metadata", {}).get("tags", []),
                        "attachments": record.get("metadata", {}).get("zd_metadata", {}).get("attachments", []),
                    }
                })
    return metadata


def find_latest_vectordata_file():
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{storage_prefix}/synced/")
        files = response.get('Contents', [])
        latest_key = None
        latest_datetime = None

        for file in files:
            key = file['Key']
            match = re.match(rf'^{re.escape(storage_prefix)}/synced/vectordata_(\d{{8}}_\d{{6}}).json$', key)
            if match:
                file_datetime = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
                if latest_datetime is None or file_datetime > latest_datetime:
                    latest_datetime = file_datetime
                    latest_key = key

        if latest_key is None:
            logger.error("No files found matching the pattern.")
        return latest_key
    except Exception as e:
        logger.error(f"An error occurred while searching for the latest file: {e}")
        return None

def load_json_from_s3_and_save(bucket, key):
    if key is None:
        logger.error("Invalid key provided. Exiting function.")
        return None
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.load(response['Body'])
        logger.info(f"Data loaded from S3: {key}")
        
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
        with open(temp_file.name, 'w') as file:
            json.dump(data, file)
        
        return temp_file.name
    except Exception as e:
        logger.error(f"Error loading data from {key}: {e}")
        return None

def initialize_bedrock_client():
    try:
        session = boto3.Session()
        bedrock_runtime = session.client(
            service_name=BEDROCK_SERVICE_NAME,
            region_name=BEDROCK_REGION_NAME
        )
        return bedrock_runtime
    except Exception as e:
        logger.error(f"Fehler bei der Initialisierung des Bedrock-Clients: {e}", exc_info=True)
        exit(1)


    
def connect_to_postgres():
    connection_string = f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    engine = create_engine(connection_string)
    
    # Test the connection
    try:
        with engine.connect() as connection:
            logger.debug("Testing database connection")
            result = connection.execute(text("SELECT 1"))
            logger.debug(f"Database connection test result: {result.fetchone()}")
        logger.info("Database connection successful")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

    return engine

def process_documents_and_upload_to_pg(docs, embed, engine):
    logger.debug(f"Starting to process {len(docs)} documents")
    all_texts_for_embeddings = [doc.metadata['combined_text'] for doc in docs]
    logger.debug(f"Extracted {len(all_texts_for_embeddings)} texts for embedding")

    try:
        logger.debug("Starting embedding process")
        all_embeddings = embed.embed_documents(all_texts_for_embeddings)
        logger.debug(f"Embedding complete. Generated {len(all_embeddings)} embeddings")

        logger.debug("Creating text-embedding pairs")
        text_embedding_pairs = list(zip(all_texts_for_embeddings, all_embeddings))
        logger.debug(f"Created {len(text_embedding_pairs)} text-embedding pairs")

        logger.debug("Uploading documents to PGVector")
        PGVector.from_documents(
            documents=docs,
            embedding=embed,
            connection_string=str(engine.url),
        )
        logger.info("Upload successful")
    except Exception as e:
        logger.error(f"Error during processing and upload: {e}", exc_info=True)
        exit(1)

def main():
    logger.debug("Script started")

    try:
        # Establish database connection first
        logger.debug("Establishing database connection")
        engine = connect_to_postgres()
        logger.debug("Database connection established successfully")

        # Initialize Bedrock client (but don't create embeddings yet)
        logger.debug("Initializing Bedrock client")
        bedrock_client = initialize_bedrock_client()
        logger.debug("Bedrock client initialized successfully")

        # Load data from S3
        logger.debug("Finding latest vector data file")
        final_key = find_latest_vectordata_file()
        if not final_key:
            logger.error("No vector data file found. Exiting.")
            return

        logger.debug(f"Loading data from S3: {final_key}")
        document_path = load_json_from_s3_and_save(bucket_name, final_key)
        if not document_path:
            logger.error("Document path is invalid. Exiting.")
            return

        # Load documents
        logger.debug("Loading documents from JSON file")
        loader = JSONLoader(
            file_path=document_path,
            jq_schema='.[]',
            content_key="answer",
            metadata_func=metadata_func
        )
        docs = loader.load()
        logger.debug(f"Loaded {len(docs)} documents")

        logger.debug("About to create BedrockEmbeddings instance")
        embed = BedrockEmbeddings(
            client=bedrock_client,
            model_id=TITAN_MODEL_ID
        )
        logger.debug("BedrockEmbeddings instance created")

        # Generate embeddings and upload to PostgreSQL
        logger.debug("About to process documents and upload to PostgreSQL")
        process_documents_and_upload_to_pg(docs, embed, engine)

        # Clean up
        logger.debug(f"Removing temporary file: {document_path}")
        os.remove(document_path)
        logger.info(f"Temporary file deleted: {document_path}")
        
        logger.debug("Main function completed successfully")
    except Exception as e:
        logger.error(f"Error during processing: {e}", exc_info=True)

if __name__ == "__main__":
    main()