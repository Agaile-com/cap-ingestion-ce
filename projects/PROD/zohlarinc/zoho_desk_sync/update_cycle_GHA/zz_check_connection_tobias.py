import os
import logging
import boto3
from sqlalchemy import create_engine, text
from sqlalchemy import event
from sqlalchemy.engine import Engine

# Required parameters
rds_host = "localhost"
region = "eu-central-1"  # e.g., 'us-west-2'
port = 5432  # default MySQL port, change it for your DB engine
db_user = "agaile_aia_zohlarinc_new_user"  # The IAM user or role associated with the DB
# You may also need to include the database name depending on your setup.

# Initialize a boto3 RDS client
rds_client = boto3.client('rds', region_name=region)

# Generate the auth token
token = rds_client.generate_db_auth_token(
    DBHostname="agaile-shared-0.chikkw2ak1ld.eu-central-1.rds.amazonaws.com",
    Port=port,
    DBUsername=db_user
)

# Print the token (use it to connect to your database)
print(f"THIS IS THE AUTH TOKEN: {token}")

connection_string = f"postgresql+psycopg2://{db_user}:{token}@{rds_host}:{port}/agaile_aia_zohlarinc_new?sslmode=require"
print(connection_string)

# Enable SQLAlchemy debug logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Detailed logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set this to DEBUG for detailed logs

# Create a file handler for logging to a file (optional)
file_handler = logging.FileHandler('db_connection_debug.log')
file_handler.setLevel(logging.DEBUG)

# Create a console handler to print to stdout
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create a formatter and attach it to both handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Log the connection attempt with user information
logger.info(f"Attempting to connect to database at {rds_host}:{port}, database: agaile_aia_zohlarinc_new, user: {db_user}")

# Create the SQLAlchemy engine
engine = create_engine(connection_string, echo=True)  # echo=True enables SQL query logging

# Enable logging of the database connection lifecycle events
@event.listens_for(Engine, "connect")
def connect_listener(dbapi_connection, connection_record):
    logger.debug(f"Database connection established with user: {db_user}")

@event.listens_for(Engine, "close")
def close_listener(dbapi_connection, connection_record):
    logger.debug("Database connection closed")

@event.listens_for(Engine, "begin")
def begin_listener(conn):
    logger.debug("Transaction started")

try:
    # Attempt to connect and execute a simple query
    with engine.connect() as connection:
        logger.debug("Executing test query: SELECT 1")
        result = connection.execute(text("SELECT 1"))
        logger.info("Connection successful, result: %s", result.scalar())
except Exception as e:
    logger.error("Connection failed with error: %s", e, exc_info=True)
