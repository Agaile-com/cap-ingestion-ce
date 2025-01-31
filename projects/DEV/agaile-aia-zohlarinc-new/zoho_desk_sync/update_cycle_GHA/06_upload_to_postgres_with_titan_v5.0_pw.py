"""
Zoho Desk Data Processing and Upload Script

This script processes Zoho Desk data, creates embeddings, and uploads them to a PostgreSQL database.
It performs the following main tasks:
1. Loads environment variables
2. Connects to PostgreSQL database
3. Creates necessary table structure if it doesn't exist
4. Retrieves the latest vector data file from AWS S3
5. Processes the data and creates embeddings
6. Uploads the embeddings to the PostgreSQL database

Requirements:
- PostgreSQL with pgvector extension
- AWS S3 access
- Bedrock client for embeddings
"""

import os
import json
import boto3
import logging
import re
import tempfile

from urllib.parse import quote_plus
from langchain_core.documents import Document
from datetime import datetime
import psycopg
from langchain_aws import BedrockEmbeddings
from langchain_postgres import PGVector
from langchain_community.document_loaders import JSONLoader
from psycopg_pool import ConnectionPool
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
DB_HOST = os.getenv('POSTGRESQL_DB_HOST')
DB_PORT = os.getenv('POSTGRESQL_DB_PORT', '5432')
DB_NAME = os.getenv('POSTGRESQL_DB_NAME')
DB_USER = os.getenv('POSTGRESQL_DB_USER')
DB_PASSWORD = os.getenv('POSTGRESQL_DB_PASSWORD')
REGION = os.getenv('AWS_REGION')

# Bedrock Service Configuration from .env
BEDROCK_SERVICE_NAME = os.getenv('BEDROCK_SERVICE_NAME', 'bedrock-runtime')
BEDROCK_REGION_NAME = os.getenv('BEDROCK_REGION_NAME', 'eu-central-1')

# Titan embedding model configuration from .env
TITAN_MODEL_ID = os.getenv('TITAN_MODEL_ID', 'amazon.titan-embed-text-v1')

def check_env_vars():
    """
    Check if all required environment variables are set.
    """
    required_vars = ['S3_BUCKET_NAME', 'TENANT_NAME', 'POSTGRESQL_DB_HOST', 'POSTGRESQL_DB_NAME', 'POSTGRESQL_DB_USER', 'POSTGRESQL_DB_PASSWORD', 'POSTGRESQL_DB_PORT', 'AWS_REGION']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        exit(1)
    logger.info("All required environment variables are set")

def test_s3_access():
    """
    Test access to the S3 bucket.
    """
    try:
        s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        logger.info("S3 access successful")
    except Exception as e:
        logger.error(f"S3 access failed: {e}")
        exit(1)

def connect_to_postgres():
    """
    Establish a connection pool to the PostgreSQL database using psycopg3.
    """
    if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
        raise ValueError("One or more required environment variables are not set.")
    
    connection_string = f"postgresql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    try:
        pool = ConnectionPool(connection_string)
        logger.info("Successfully created connection pool to the database")
        return pool
    except Exception as e:
        logger.error(f"Failed to create connection pool: {str(e)}")
        raise

def test_db_connection(pool):
    """
    Test the database connection using the connection pool.
    """
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
        logger.info("Database connection test successful")
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        raise

def create_table_if_not_exists(pool):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                # Check if the table already exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'langchain_pg_embedding'
                    );
                """)
                table_exists = cur.fetchone()[0]

                if not table_exists:
                    # Create the table if it doesn't exist
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS public.langchain_pg_embedding
                        (
                            id character varying NOT NULL,
                            collection_id uuid,
                            embedding vector,
                            document text,
                            cmetadata jsonb,
                            CONSTRAINT langchain_pg_embedding_pkey PRIMARY KEY (id)
                        );
                    """)
                    logger.info("Table langchain_pg_embedding created")

                # Check if the GIN index on cmetadata exists
                cur.execute("""
                    SELECT COUNT(*) FROM pg_indexes
                    WHERE indexname = 'ix_cmetadata_gin'
                    AND tablename = 'langchain_pg_embedding';
                """)
                gin_index_exists = cur.fetchone()[0]

                if not gin_index_exists:
                    # Create the GIN index if it doesn't exist
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS ix_cmetadata_gin
                        ON public.langchain_pg_embedding USING gin
                        (cmetadata jsonb_path_ops);
                    """)
                    logger.info("GIN index created on cmetadata column")

                conn.commit()
                logger.info("Table langchain_pg_embedding and necessary indexes verified or created")
                return True
            except Exception as e:
                conn.rollback()
                logger.error(f"Error in create_table_if_not_exists: {str(e)}")
                return False

def find_latest_vectordata_file():
    """
    Find the latest vector data file in the S3 bucket.
    """
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
    """
    Load JSON data from S3 and save it to a temporary file.
    """
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
    """
    Initialize the Bedrock client for embeddings.
    """
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



def metadata_func(record: dict, metadata: dict) -> dict:
    metadata.update({
        "namespace": "abc",
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


def process_documents_and_upload_to_pg(docs, embed, pool):
    all_texts_for_embeddings = [doc.metadata.get('combined_text', doc.page_content) for doc in docs]
    all_metadata = [doc.metadata for doc in docs]
    all_answers = [doc.metadata.get('answer', doc.page_content) for doc in docs]

    try:
        all_embeddings = embed.embed_documents(all_texts_for_embeddings)
        
        # Create connection string
        connection_string = f"postgresql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        
        # Initialize PGVector
        pgvector = PGVector(
            embeddings=embed,
            collection_name="langchain",
            connection=connection_string,
        )
        
        # Create Document objects
        documents = [
            Document(page_content=answer, metadata={**metadata, 'embedding': embedding})
            for answer, metadata, embedding in zip(all_answers, all_metadata, all_embeddings)
        ]
        
        # Add documents
        pgvector.add_documents(documents)
        
        logger.info("Upload successful")
    except Exception as e:
        logger.error(f"Error during upload: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error args: {e.args}")
        
        if hasattr(e, '__cause__'):
            logger.error(f"Cause: {str(e.__cause__)}")
        
        raise
def main():
    """
    Main function to orchestrate the entire process.

    This function performs the following steps:
    1. Check environment variables and establish database connection.
    2. Verify and create necessary table structure.
    3. Find the latest vector data file in S3.
    4. Load and process the data.
    5. Initialize the Bedrock client for embeddings.
    6. Clear existing data from the database.
    7. Process documents and upload new data to the database.
    """
    logger.info("Starting main function")
    logger.info(f"Attempting to connect to: {DB_HOST}:{DB_PORT}/{DB_NAME} as user {DB_USER}")

    try:
        # Connect to the database
        pool = connect_to_postgres()
        logger.info("Connected to PostgreSQL")
        test_db_connection(pool)
        logger.info("Database connection test passed")

        # Verify and create table structure
        if not create_table_if_not_exists(pool):
            logger.error("Failed to verify or create necessary table structure. Exiting.")
            return

        # Find the latest vector data file
        final_key = find_latest_vectordata_file()
        if not final_key:
            logger.error("No vector data file found. Exiting.")
            return

        # Load data from S3
        document_path = load_json_from_s3_and_save(bucket_name, final_key)
        if not document_path:
            logger.error("Document path is invalid. Exiting.")
            return

        # Load and process documents
        loader = JSONLoader(
            file_path=document_path,
            jq_schema='.[]',
            content_key="answer",
            metadata_func=metadata_func
        )
        docs = loader.load()
        logger.info(f"Loaded {len(docs)} documents")

        # Initialize Bedrock client
        embed = initialize_bedrock_client()
        logger.info("Initialized Bedrock client")

        # Clear existing data
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM public.langchain_pg_embedding")
                conn.commit()
            logger.info("Cleared previous data from the table.")

        # Process and upload documents
        process_documents_and_upload_to_pg(docs, embed, pool)

        # Clean up temporary file
        os.remove(document_path)
        logger.info(f"Temporary file deleted: {document_path}")
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}", exc_info=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Unhandled exception in script: {str(e)}", exc_info=True)
