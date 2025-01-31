import os
import logging
import boto3
from urllib.parse import quote_plus
from langchain_aws import BedrockEmbeddings
from langchain_postgres import PGVector
from langchain_core.documents import Document
from pathlib import Path
import sys
import psycopg


# Dynamically add the parent directory to sys.path
script_dir = Path(__file__).resolve().parent
parent_dir = script_dir.parent
sys.path.append(str(parent_dir))

# Now import utils after sys.path is set
from utils import load_env

# Load environment variables
load_env()

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
DB_HOST = os.getenv('POSTGRESQL_DB_HOST')
DB_PORT = os.getenv('POSTGRESQL_DB_PORT', '5432')
DB_NAME = os.getenv('POSTGRESQL_DB_NAME')
DB_USER = os.getenv('POSTGRESQL_DB_USER')
BEDROCK_SERVICE_NAME = os.getenv('BEDROCK_SERVICE_NAME', 'bedrock-runtime')
BEDROCK_REGION_NAME = os.getenv('BEDROCK_REGION_NAME', 'eu-central-1')
TITAN_MODEL_ID = os.getenv('TITAN_MODEL_ID', 'amazon.titan-embed-text-v1')
SEARCH_TERM = os.getenv('SEARCH_TERM')

def initialize_bedrock_client():
    """Initialize the Bedrock client for embeddings."""
    try:
        logger.debug("Initializing Bedrock client...")
        session = boto3.Session()
        bedrock_runtime = session.client(
            service_name=BEDROCK_SERVICE_NAME,
            region_name=BEDROCK_REGION_NAME
        )
        embed = BedrockEmbeddings(client=bedrock_runtime, model_id=TITAN_MODEL_ID)
        logger.info("Successfully initialized Bedrock client")
        return embed
    except Exception as e:
        logger.error(f"Error initializing Bedrock client: {e}", exc_info=True)
        raise

def setup_pgvector(embed):
    """Set up the PGVector instance with enhanced logging."""
    try:
        logger.debug("Setting up PGVector with IAM authentication...")
        rds_client = boto3.client('rds')
        token = rds_client.generate_db_auth_token(
            DBHostname=DB_HOST,
            Port=DB_PORT,
            DBUsername=DB_USER,
            Region=BEDROCK_REGION_NAME
        )
        logger.debug("Generated IAM authentication token")

        connection_string = f"postgresql://{DB_USER}:{quote_plus(token)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        
        # Log connection details (be careful not to log the full token)
        logger.debug(f"Attempting to connect to: {DB_HOST}:{DB_PORT}/{DB_NAME} as user {DB_USER}")
        
        # Test the connection
        logger.debug("Testing database connection with IAM token...")
        try:
            with psycopg.connect(connection_string, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version()")
                    version = cur.fetchone()[0]
                    logger.debug(f"Successfully connected to the database. PostgreSQL version: {version}")
        except Exception as conn_error:
            logger.error(f"Connection test failed: {conn_error}")
            # Log more details about the error
            logger.error(f"Error type: {type(conn_error).__name__}")
            logger.error(f"Error details: {str(conn_error)}")
            raise

        pgvector = PGVector(
            embeddings=embed,
            collection_name="langchain_pg_embedding",
            connection_string=connection_string,
        )
        logger.info("Successfully set up PGVector with IAM authentication")
        return pgvector
    except Exception as e:
        logger.error(f"Error setting up PGVector: {e}", exc_info=True)
        raise

def generate_search_vector(embed, search_term):
    """Generate vector for the search term using Bedrock."""
    try:
        logger.debug(f"Generating search vector for term: '{search_term}'")
        search_vector = embed.embed_query(search_term)
        logger.info(f"Successfully generated vector for search term: '{search_term}'")
        return search_vector
    except Exception as e:
        logger.error(f"Error generating search vector: {e}", exc_info=True)
        raise

def perform_similarity_search(vector_store, search_vector, k=5):
    """Perform a similarity search using PGVector with a given vector."""
    try:
        logger.debug(f"Performing similarity search with k={k}")
        results = vector_store.similarity_search_by_vector(search_vector, k=k)
        logger.info(f"Similarity search completed. Found {len(results)} results.")
        return results
    except Exception as e:
        logger.error(f"Error during similarity search: {e}", exc_info=True)
        raise

def main():
    try:
        logger.info("Starting main function")
        
        bedrock_embed = initialize_bedrock_client()
        vector_store = setup_pgvector(bedrock_embed)
        search_vector = generate_search_vector(bedrock_embed, SEARCH_TERM)
        results = perform_similarity_search(vector_store, search_vector)
        
        if results:
            for i, result in enumerate(results, 1):
                logger.info(f"Result {i}:")
                logger.info(f"Content: {result.page_content[:100]}...")
                logger.info(f"Metadata: {result.metadata}")
                logger.info("-" * 50)
        else:
            logger.info("No results found.")
        
        logger.info("Main function completed successfully")
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)

if __name__ == "__main__":
    main()