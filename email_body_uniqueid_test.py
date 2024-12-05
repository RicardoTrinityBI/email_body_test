import os
import json
import time
import logging
import snowflake.connector
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

snowflake_user = os.getenv('SNOWFLAKE_USER')
snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')

# Create the service account credentials file
SERVICE_ACCOUNT_JSON = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
SERVICE_ACCOUNT_FILE = 'google_creds.json'

with open(SERVICE_ACCOUNT_FILE, 'w') as f:
    f.write(SERVICE_ACCOUNT_JSON)

# Scopes required for accessing Google Workspace and Gmail data
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly'
]

# Load the service account credentials
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Configure logging
logging.basicConfig(filename='email_fetch_errors.log', level=logging.ERROR)

def list_messages(user_email, max_results=100):
    """List messages in the user's mailbox with pagination for the previous day."""
    all_messages = []
    page_token = None

    # Calculate the date for 'yesterday'
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y/%m/%d')
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    # Query for emails only from the previous day
    query = f"after:{yesterday} before:{today}"

    # Delegate the credentials to the user
    delegated_credentials = credentials.with_subject(user_email)
    gmail_service = build('gmail', 'v1', credentials=delegated_credentials)

    while len(all_messages) < max_results:
        try:
            results = gmail_service.users().messages().list(
                userId='me',
                pageToken=page_token,
                q=query,  # Query for filtering by date
                maxResults=min(500, max_results - len(all_messages))  # Gmail API supports max 500
            ).execute()
            
            messages = results.get('messages', [])
            all_messages.extend(messages)

            page_token = results.get('nextPageToken')
            if not page_token:
                break

            # Avoid hitting rate limits
            time.sleep(0.5)
        except HttpError as error:
            logging.error(f"An error occurred while listing messages for {user_email}: {error}")
            break

    return all_messages


def get_message_details(message_id, user_email, retries=3):
    """Get only the id and email body of a specific message."""
    delegated_credentials = credentials.with_subject(user_email)
    gmail_service = build('gmail', 'v1', credentials=delegated_credentials)

    for attempt in range(retries):
        try:
            message = gmail_service.users().messages().get(
                userId='me', 
                id=message_id
            ).execute()
            payload = message.get('payload', {})
            
            # Initialize details with only id and email body
            details = {
                'id': message_id,
                'email_body': '',
                'inserted_date': datetime.now(timezone.utc).strftime('%Y-%m-%d')
            }

            # Extract the email body from the payload
            if 'parts' in payload:
                for part in payload['parts']:
                    mime_type = part.get('mimeType')
                    if mime_type == 'text/plain':
                        details['email_body'] = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                        break  # Stop at the first found plain text part
                    elif mime_type == 'text/html':
                        details['email_body'] = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
            else:
                # If no parts, the body is in the 'body' key directly
                details['email_body'] = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8')
            
            return details
        except HttpError as error:
            logging.error(f"Attempt {attempt+1}: An error occurred while getting details for message {message_id} from {user_email}: {error}")
            time.sleep(2)  # Wait before retrying

    logging.error(f"Failed to fetch details for message {message_id} from {user_email} after {retries} attempts.")
    return None

def save_to_snowflake(data, conn, batch_size=1000):
    """Save the id and email body to the Snowflake EMAIL_BODIES_TEST table in batches."""
    if data:
        cursor = conn.cursor()
        batch = []
        for row in data:
            row['inserted_date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            batch.append(row)
            if len(batch) >= batch_size:
                cursor.executemany(
                    """
                    INSERT INTO TEST123 (id, email_body, inserted_date)
                    VALUES (%(id)s, %(email_body)s, %(inserted_date)s)
                    """,
                    batch
                )
                batch = []
        if batch:
            cursor.executemany(
                """
                INSERT INTO TEST123 (id, email_body, inserted_date)
                VALUES (%(id)s, %(email_body)s, %(inserted_date)s)
                """,
                batch
            )
        conn.commit()
        cursor.close()

def fetch_details_concurrently(messages, user_email):
    """Fetch message details concurrently."""
    email_details = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_message = {executor.submit(get_message_details, message['id'], user_email): message for message in messages}
        for future in as_completed(future_to_message):
            message = future_to_message[future]
            try:
                details = future.result()
                if details:
                    email_details.append(details)
            except Exception as exc:
                logging.error(f"An error occurred while fetching details for message {message['id']} from {user_email}: {exc}")
    return email_details

def connect_to_snowflake():
    """Establish a connection to Snowflake."""
    return snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )

if __name__ == '__main__':
    email_list = os.getenv('EMAILS')
    users = [email.strip() for email in email_list.split(',')] if email_list else []

    conn = connect_to_snowflake()
    all_email_details = []

    for user_email in users:
        try:
            print(f"Fetching unique threads for {user_email}...")
            messages = list_messages(user_email, max_results=100)
            print(f"Found {len(messages)} for {user_email}.")
            
            email_details = fetch_details_concurrently(messages, user_email)
            all_email_details.extend(email_details)
        except RefreshError as refresh_error:
            logging.error(f"Failed to refresh credentials for {user_email}: {refresh_error}")
            print(f"Skipping {user_email} due to authentication issues.")
            continue

    save_to_snowflake(all_email_details, conn)
    print('Email details have been saved to the Snowflake EMAIL_BODIES_TEST table.')
    conn.close()
