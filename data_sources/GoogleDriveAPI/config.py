import os

from logger import log

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

source_name = "google.drive.api"
webhook_route = "/webhooks/google_drive"

def authenticate():
    TOKEN_PATH = 'data_sources/GoogleDriveAPI/token.json'
    CREDS_PATH = 'data_sources/GoogleDriveAPI/credentials.json'
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                log("Authentication credentials refreshed.", "info", source_name)
                
                with open(TOKEN_PATH, 'w') as token:
                    token.write(creds.to_json())
            except:
                log("Authentication failed. Need to generate a new token.", "error", source_name)

    return build('drive', 'v3', credentials=creds)