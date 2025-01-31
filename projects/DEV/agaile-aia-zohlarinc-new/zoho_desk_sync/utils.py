from dotenv import load_dotenv
from pathlib import Path

def load_env():
    # Locate the .env file in the same directory as utils.py
    env_path = Path(__file__).resolve().parent / '.env'
    
    # Check if the .env file exists
    if not env_path.exists():
        raise FileNotFoundError(f".env file not found at {env_path}")
    
    # Load environment variables
    load_dotenv(env_path)