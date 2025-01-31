import os
import logging
import boto3
from urllib.parse import quote_plus
from langchain_aws import BedrockEmbeddings
from langchain_postgres import PGVector
from langchain_core.documents import Document
from pathlib import Path
import sys

# Dynamically add the parent directory to sys.path
script_dir = Path(__file__).resolve().parent
parent_dir = script_dir.parent
sys.path.append(str(parent_dir))

# Now import utils after sys.path is set
from utils import load_env

# Load environment variables
load_env()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
DB_HOST = os.getenv('POSTGRESQL_DB_HOST')
DB_PORT = os.getenv('POSTGRESQL_DB_PORT', '5432')
DB_NAME = os.getenv('POSTGRESQL_DB_NAME')
DB_USER = os.getenv('POSTGRESQL_DB_USER')
DB_PASSWORD = os.getenv('POSTGRESQL_DB_PASSWORD')
BEDROCK_SERVICE_NAME = os.getenv('BEDROCK_SERVICE_NAME', 'bedrock-runtime')
BEDROCK_REGION_NAME = os.getenv('BEDROCK_REGION_NAME', 'eu-central-1')
TITAN_MODEL_ID = os.getenv('TITAN_MODEL_ID', 'amazon.titan-embed-text-v1')
SEARCH_TERM = os.getenv('SEARCH_TERM')

def initialize_bedrock_client():
    """Initialize the Bedrock client for embeddings."""
    try:
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
    """Set up the PGVector instance."""
    try:
        connection_string = f"postgresql://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        
        pgvector = PGVector(
            embeddings=embed,
            collection_name="langchain_pg_embedding",
            connection=connection_string,
        )
        logger.info("Successfully set up PGVector")
        return pgvector
    except Exception as e:
        logger.error(f"Error setting up PGVector: {e}", exc_info=True)
        raise

def generate_search_vector(embed, search_term):
    """Generate vector for the search term using Bedrock."""
    try:
        search_vector = embed.embed_query(search_term)
        logger.info(f"Successfully generated vector for search term: '{search_term}'")
        return search_vector
    except Exception as e:
        logger.error(f"Error generating search vector: {e}", exc_info=True)
        raise

def perform_similarity_search(vector_store, search_vector, k=5):
    """Perform a similarity search using PGVector with a given vector."""
    try:
        results = vector_store.similarity_search_by_vector(search_vector, k=k)
        return results
    except Exception as e:
        logger.error(f"Error during similarity search: {e}", exc_info=True)
        raise

def main():
    try:
        # Initialize Bedrock client
        bedrock_embed = initialize_bedrock_client()
        
        # Set up PGVector
        vector_store = setup_pgvector(bedrock_embed)
        
        # Generate vector for search term
        search_vector = generate_search_vector(bedrock_embed, SEARCH_TERM)
        
        # Perform similarity search
        results = perform_similarity_search(vector_store, search_vector)
        
        # Output results
        if results:
            for i, result in enumerate(results, 1):
                print(f"Result {i}:")
                print(f"Content: {result.page_content[:100]}...")  # Show only the first 100 characters
                print(f"Metadata: {result.metadata}")
                print("-" * 50)
        else:
            print("No results found.")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)

if __name__ == "__main__":
    main()