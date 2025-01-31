"""
Script to fetch the refresh token from Zoho OAuth2.0.

Steps:
1. Load environment variables for client ID, client secret, authorization code, and Zoho token URL.
2. Send a POST request to Zoho OAuth endpoint to exchange the authorization code for a refresh token.
3. Log the success or failure of the request.

Environment Variables:
- CLIENT_ID: Client ID for Zoho OAuth.
- CLIENT_SECRET: Client Secret for Zoho OAuth.
- AUTHORIZATION_CODE: Authorization code obtained after the user grants access.
- ZOHO_TOKEN_URL: Zoho token URL to request access and refresh tokens.
"""

import requests
import logging
import os
from utils import load_env

# Load environment variables using the helper function
load_env()
# Configure logging for info and error messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_refresh_token(client_id, client_secret, authorization_code, token_url):
    """
    Exchange the authorization code for a refresh token from Zoho OAuth2.0.

    Args:
        client_id (str): Client ID from Zoho Developer Console.
        client_secret (str): Client Secret from Zoho Developer Console.
        authorization_code (str): Authorization code received after user grants permission.
        token_url (str): URL to request the access token from Zoho OAuth.

    Returns:
        str or None: The refresh token if the request was successful, None otherwise.
    """
    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "authorization_code",
        "code": authorization_code
    }

    try:
        logging.info("Sending request to Zoho to fetch refresh token...")
        response = requests.post(token_url, params=params)
        response.raise_for_status()  # Raise an error for bad responses (4xx and 5xx)
        
        refresh_token = response.json().get("refresh_token")
        if refresh_token:
            logging.info("Successfully fetched the refresh token.")
            return refresh_token
        else:
            logging.error("Refresh token not found in the response.")
            logging.error(f"Response body: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during token request: {e}")
        return None

# Fetch client ID, client secret, authorization code, and token URL from environment variables
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
authorization_code = os.getenv('AUTHORIZATION_CODE')
token_url = os.getenv('ZOHO_TOKEN_URL')

if not all([client_id, client_secret, authorization_code, token_url]):
    logging.error("CLIENT_ID, CLIENT_SECRET, AUTHORIZATION_CODE, or ZOHO_TOKEN_URL not found in environment variables.")
else:
    # Get the refresh token
    refresh_token = get_refresh_token(client_id, client_secret, authorization_code, token_url)

    if refresh_token:
        logging.info(f"Refresh Token: {refresh_token}")
    else:
        logging.error("Failed to obtain the refresh token.")
