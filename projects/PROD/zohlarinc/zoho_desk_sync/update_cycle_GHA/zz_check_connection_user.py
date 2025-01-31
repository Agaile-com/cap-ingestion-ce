
import os
import logging
import boto3
from sqlalchemy import create_engine, text
from sqlalchemy import event
from sqlalchemy.engine import Engine



# Required parameters
rds_host = "127.0.0.1"
region = "eu-central-1"# e.g., 'us-west-2'
port = 5432# default MySQL port, change it for your DB engine
DB_USER = "agaile_aia_zohlarinc_new_user"# The IAM user or role associated with the DB# You may also need to include the database name depending on your setup.# Initialize a boto3 RDS client
rds_client = boto3.client('rds', region_name=region)

# Generate the auth token

token = rds_client.generate_db_auth_token(
    DBHostname=rds_host,
    Port=port,
    DBUsername=DB_USER
)

print (token)
 
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

# SQLAlchemy connection configuration
DB_HOST = '127.0.0.1'
#DB_HOST = 'agaile-shared-0.chikkw2ak1ld.eu-central-1.rds.amazonaws.com'
DB_PORT = '5432'
DB_NAME = 'agaile_aia_zohlarinc_new'
# DB_USER = 'postgres'
# DB_PASSWORD = 'lp}(g*U?!v6$j0]a->HY}:r15YQL'  # Hard-coded for testing
DB_PASSWORD = token  # Hard-coded for testing

# Create the connection string
connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
print (connection_string)

# Log the connection attempt
logger.info(f"Attempting to connect to database at {DB_HOST}:{DB_PORT}, database: {DB_NAME}, user: {DB_USER}")

# Create the SQLAlchemy engine
engine = create_engine(connection_string, echo=True)  # echo=True enables SQL query logging

# Enable logging of the database connection lifecycle events
@event.listens_for(Engine, "connect")
def connect_listener(dbapi_connection, connection_record):
    logger.debug("Database connection established")

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