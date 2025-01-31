"""
S3 Bucket Content Listing Script

This script lists the contents of an S3 bucket, including object keys and sizes, using the boto3 client.
It ensures environment-driven configuration for flexibility across different environments and adds enhanced logging for debugging.

Process:
1. **S3 Client Creation**: Create an S3 client using boto3.
2. **List Objects**: List all objects in the specified S3 bucket.
3. **Logging**: Log each object’s key and size. Log an appropriate message if the bucket is empty.
4. **Error Handling**: Catch and log any exceptions during the process.

Environment Variables:
- `S3_BUCKET_NAME`: The name of the S3 bucket to be inspected.

"""

import boto3
import logging
import os
from utils import load_env

# Load environment variables using the helper function
load_env()
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def list_bucket_contents(bucket_name):
    """
    Lists the contents of an S3 bucket, including object keys and sizes.

    Args:
        bucket_name (str): The name of the S3 bucket.

    Raises:
        Exception: If there's an issue with the S3 client or listing the bucket contents.
    """
    try:
        # Create an S3 client
        s3 = boto3.client('s3')
        
        logger.info(f"Listing contents of S3 bucket: {bucket_name}")
        
        # List all objects in the bucket
        response = s3.list_objects_v2(Bucket=bucket_name)

        # Check if the bucket contains any objects
        if 'Contents' in response:
            logger.info(f"Objects found in bucket '{bucket_name}':")
            for item in response['Contents']:
                logger.info(f"Key: {item['Key']} - Size: {item['Size']} Bytes")
        else:
            logger.info(f"No objects found in bucket '{bucket_name}'.")
    
    except Exception as e:
        logger.error(f"An error occurred while listing the contents of the bucket: {e}")
        raise

if __name__ == "__main__":
    """
    Main function that retrieves the S3 bucket name from environment variables and lists its contents.
    """
    # Retrieve the S3 bucket name from the environment variable
    bucket_name = os.getenv('S3_BUCKET_NAME')

    if not bucket_name:
        logger.error("S3_BUCKET_NAME environment variable is not set.")
    else:
        # List the contents of the specified S3 bucket
        list_bucket_contents(bucket_name)
