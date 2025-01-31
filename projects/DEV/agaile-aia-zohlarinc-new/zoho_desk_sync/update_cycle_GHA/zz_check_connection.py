import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy import event
from sqlalchemy.engine import Engine

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
DB_USER = 'postgres'
DB_PASSWORD = 'aQeCCK1q!UcS2q*6!ky:Y#Ca_9lu'  # Hard-coded for testing

# Create the connection string
connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

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