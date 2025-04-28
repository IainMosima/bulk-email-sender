import os
import base64
import re
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If modifying these SCOPES, delete the file token-pension.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


def get_gmail_service(token_file, credentialsFile='credentials.json'):
    """Get authenticated Gmail API service."""
    creds = None
    # The file token-pension.json stores the user's access and refresh tokens
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_info(
            json.loads(open(token_file).read()), SCOPES)

    # If credentials don't exist or are invalid, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save credentials for the next run
        with open(token_file, 'w') as token:
            token.write(creds.to_json())

    return build('gmail', 'v1', credentials=creds)


def fetch_failed_emails(token_file):
    """Fetch delivery failure notifications from Gmail."""
    service = get_gmail_service(token_file)

    # Search for delivery failure notifications
    query = "Your message wasn't delivered OR subject:\"Delivery Status Notification\""
    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])

    patterns = [
        # Pattern with asterisks (original)
        r"Your message wasn't delivered to\s+\*\*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\*\*",
        # Pattern without asterisks (original fallback)
        r"Your message wasn't delivered to\s+([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
        # Match Final-Recipient format
        r"Final-Recipient:.*?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
        # Match X-Failed-Recipients format
        r"X-Failed-Recipients:\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"
    ]

    if not messages:
        return []

    failed_emails = []
    # Print the first message for debugging (you can remove this in production)
    # print ("The message is: ", service.users().messages().get(userId='me', id=messages[0]['id']).execute())

    for message in messages:
        msg = service.users().messages().get(userId='me', id=message['id']).execute()

        # Get email headers
        headers = msg['payload']['headers']
        subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
        date = next((h['value'] for h in headers if h['name'] == 'Date'), 'No Date')

        # First check X-Failed-Recipients header
        failed_recipient = next((h['value'] for h in headers if h['name'] == 'X-Failed-Recipients'), "Unknown")

        # If failed_recipient is still "Unknown", try to extract from body
        if failed_recipient == "Unknown" and 'parts' in msg['payload']:
            for part in msg['payload']['parts']:
                # Check text/plain parts
                if part.get('mimeType') == 'text/plain' and 'data' in part['body']:
                    data = base64.urlsafe_b64decode(part['body']['data']).decode()
                    print("The data is: ", data)

                    # Try multiple regex patterns


                    for pattern in patterns:
                        email_match = re.search(pattern, data)
                        if email_match:
                            failed_recipient = email_match.group(1)
                            break

                    if failed_recipient != "Unknown":
                        break

                # Check HTML parts if we haven't found the email yet
                elif part.get('mimeType') == 'text/html' and 'data' in part['body'] and failed_recipient == "Unknown":
                    data = base64.urlsafe_b64decode(part['body']['data']).decode()

                    # HTML-specific patterns
                    html_patterns = [
                        # HTML bold tag pattern (seen in your sample)
                        r"<b>([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})</b>",
                        # Any email address as fallback
                        r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"
                    ]

                    for pattern in html_patterns:
                        email_match = re.search(pattern, data)
                        if email_match:
                            failed_recipient = email_match.group(1)
                            break

                    if failed_recipient != "Unknown":
                        break

                # Check for nested parts
                elif 'parts' in part and failed_recipient == "Unknown":
                    for nested_part in part['parts']:
                        if nested_part.get('mimeType') == 'text/plain' and 'data' in nested_part['body']:
                            data = base64.urlsafe_b64decode(nested_part['body']['data']).decode()

                            # Use the same patterns
                            for pattern in patterns:
                                email_match = re.search(pattern, data)
                                if email_match:
                                    failed_recipient = email_match.group(1)
                                    break

                            if failed_recipient != "Unknown":
                                break

        failed_emails.append({
            'Date': date,
            'Subject': subject,
            'Failed Recipient': failed_recipient,
            'Message ID': message['id']
        })

    return failed_emails


if __name__ == '__main__':
    import json

    # TODO: Always change here
    token_file = "token-governance.json"

    failed_emails = fetch_failed_emails(token_file)
    where_to_save = "failed_emails-governance.csv"

    if not failed_emails:
        print("No delivery failure notifications found.")
    else:
        # Create a DataFrame for better viewing
        df = pd.DataFrame(failed_emails)
        df.drop_duplicates(inplace=True, subset=['Failed Recipient'])
        print(f"Found {len(failed_emails)} delivery failure notifications:")
        print(df)

        # Optionally save to CSV
        df.to_csv(where_to_save, index=False)
        print("Results saved to ", where_to_save)