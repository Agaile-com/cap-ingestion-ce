# utils.py
from dotenv import load_dotenv
from pathlib import Path

def load_env():
    # Locate the .env file relative to the project root
    project_root = Path(__file__).resolve().parent.parent  # Adjust as needed
    env_path = project_root / '.env'
    load_dotenv(env_path)