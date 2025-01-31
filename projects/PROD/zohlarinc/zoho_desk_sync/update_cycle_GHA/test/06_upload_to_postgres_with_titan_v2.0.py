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
from langchain_aws import BedrockEmbeddings
from langchain_community.vectorstores import PGVector
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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS S3 Configuration
s3_client = boto3.client('s3')
bucket_name = os.getenv('S3_BUCKET_NAME')
storage_prefix = f"{os.getenv('TENANT_NAME')}/zohodesk-data"

# PostgreSQL configuration
DB_HOST = os.getenv('POSTGRESQL_DB_HOST')  # Use the local forwarded address
DB_PORT = int(os.getenv('POSTGRESQL_DB_PORT', '5432'))
DB_NAME = os.getenv('POSTGRESQL_DB_NAME')
DB_USER = os.getenv('POSTGRESQL_DB_USER')
#DB_PASSWORD = 'aQeCCK1q!UcS2q*6!ky:Y#Ca_9lu'
REGION = os.getenv('AWS_REGION')

# Bedrock Service Configuration from .env
BEDROCK_SERVICE_NAME = os.getenv('BEDROCK_SERVICE_NAME', 'bedrock-runtime')
BEDROCK_REGION_NAME = os.getenv('BEDROCK_REGION_NAME', 'eu-central-1')

# Titan embedding model configuration from .env
TITAN_MODEL_ID = os.getenv('TITAN_MODEL_ID', 'amazon.titan-embed-text-v2:0')

def check_env_vars():
    required_vars = ['S3_BUCKET_NAME', 'TENANT_NAME', 'POSTGRESQL_DB_NAME', 'POSTGRESQL_DB_USER',  'POSTGRESQL_DB_PORT', 'AWS_REGION']
    for var in required_vars:
        if not os.getenv(var):
            logger.error(f"Missing environment variable: {var}")
            exit(1)
    logger.info("All required environment variables are set")

def metadata_func(record: dict, metadata: dict) -> dict:

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
    logger.info(f"Searching for files in bucket: {bucket_name}")
    logger.info(f"Using prefix: {storage_prefix}/synced/")
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{storage_prefix}/synced/")
        logger.info(f"S3 response: {response}")
        files = response.get('Contents', [])
        logger.info(f"Found {len(files)} files")
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
            logger.error(f"No files found matching the pattern in {storage_prefix}/synced/")
        return latest_key
    except Exception as e:
        logger.error(f"An error occurred while searching for the latest file: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error args: {e.args}")
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
        embed = BedrockEmbeddings(client=bedrock_runtime, model_id=TITAN_MODEL_ID)
        return embed
    except Exception as e:
        logger.error(f"Error during Titan Embeddings initialization: {e}", exc_info=True)
        exit(1)

def connect_to_postgres():
    from urllib.parse import quote_plus
    DB_PASSWORD = quote_plus('aQeCCK1q!UcS2q*6!ky:Y#Ca_9lu')

    connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)
    return engine

def test_db_connection(engine):
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            logger.info("Database connection successful")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error args: {e.args}")
        if hasattr(e, 'orig'):
            logger.error(f"Original error: {e.orig}")
        exit(1)

def test_s3_access():
    try:
        s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        logger.info("S3 access successful")
    except Exception as e:
        logger.error(f"S3 access failed: {e}")
        exit(1)

def process_documents_and_upload_to_pg(docs, embed, engine):
    all_texts_for_embeddings = [doc.metadata.get('combined_text', doc.page_content) for doc in docs]
    all_metadata = [doc.metadata for doc in docs]

    try:
        # Get the embeddings for each document text
        all_embeddings = embed.embed_documents(all_texts_for_embeddings)

        # Create the text-embedding pairs
        text_embedding_pairs = list(zip(all_texts_for_embeddings, all_embeddings))

        # Use PGVector's from_embeddings method
        PGVector.from_embeddings(
            text_embeddings=text_embedding_pairs,
            embedding=embed,  # Your embedding function
            metadatas=all_metadata,
            connection_string=str(engine.url),
            collection_name="langchain_pg_embedding",
            pre_delete_collection=False,
            create_extension=False  # Disable extension creation
        )
        logger.info("Upload successful")
    except Exception as e:
        logger.error(f"Error during upload: {e}", exc_info=True)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error args: {e.args}")
        exit(1)

def main():
    check_env_vars()
    test_s3_access()

    logger.info(f"Attempting to connect to: {DB_HOST}:{DB_PORT}/{DB_NAME} as user {DB_USER}")

    final_key = find_latest_vectordata_file()
    if not final_key:
        logger.error("No vector data file found. Exiting.")
        exit(1)

    document_path = load_json_from_s3_and_save(bucket_name, final_key)
    if not document_path:
        logger.error("Document path is invalid. Exiting.")
        exit(1)

    try:
        loader = JSONLoader(
            file_path=document_path,
            jq_schema='.[]',
            content_key="answer",
            metadata_func=metadata_func
        )
        docs = loader.load()

        embed = initialize_bedrock_client()

        engine = connect_to_postgres()
        test_db_connection(engine)

        with engine.connect() as connection:
            connection.execute(text("CREATE TABLE IF NOT EXISTS public.langchain_pg_embedding (id TEXT);"))
            connection.execute(text("ALTER TABLE public.langchain_pg_embedding ADD COLUMN IF NOT EXISTS id TEXT;"))

        with engine.connect() as connection:
            connection.execute(text("DELETE FROM public.langchain_pg_embedding"))
            logger.info("Cleared previous data from the table.")

        process_documents_and_upload_to_pg(docs, embed, engine)

        os.remove(document_path)
        logger.info(f"Temporary file deleted: {document_path}")
    except Exception as e:
        logger.error(f"Error during processing: {e}")

if __name__ == "__main__":
    main()