import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os
from datetime import datetime
import time
import logging
import dotenv
import argparse
import random
import json
from dotenv import load_dotenv

# Set up logging
# Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a file handler
file_handler = logging.FileHandler('bulk-email-sender.log')
file_handler.setLevel(logging.INFO)

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Get the root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Add the handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

class BulkEmailSender:
    def __init__(self, smtp_server, smtp_port, username, password, sender_email, sender_name=None):        
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.sender_email = sender_email
        self.sender_name = sender_name
        self.session = None
        self.connection_attempts = 0
        self.max_connection_attempts = 3
        self.retry_delay = 60  # seconds
    
    def connect(self):
        try:
            self.connection_attempts += 1
            self.session = smtplib.SMTP(self.smtp_server, self.smtp_port)
            self.session.ehlo()
            self.session.starttls() # enable encryption
            self.session.ehlo()
            self.session.login(self.username, self.password)
            self.connection_attempts = 0  # Reset counter on successful connection
            logging.info("Successfully connected to SMTP server")
        except Exception as e:
            logging.error(f"Failed to connect to SMTP server: {e}")
            return False
        return True
    
    def disconnect(self):
        if self.session:
            self.session.quit()
            self.session = None
            logging.info("Disconnected from SMTP server")
    
    def create_message(self, recipient_email, subject, html_content, text_content=None, attachment_paths=None, cc=None, bcc=None):
        msg = MIMEMultipart("alternative")
        msg["subject"] = subject
        msg["From"] = f"{self.sender_name} <{self.sender_email}>"
        msg["To"] = recipient_email
        
        if cc:
            msg["Cc"] = cc
        if bcc:
            msg["Bcc"] = bcc
        
        if text_content:
            msg.attach(MIMEText(text_content, "plain"))
        msg.attach(MIMEText(html_content, "html"))
        
        if attachment_paths:
            for file_path in attachment_paths:
                if os.path.exists(file_path):
                    with open(file_path, "rb") as f:
                        attachment = MIMEApplication(f.read(), Name=os.path.basename(file_path))
                        attachment["Content-Disposition"] = f"attachment; filename={os.path.basename(file_path)}"
                        msg.attach(attachment)
                else:
                    logging.warning(f"Attachment not found: {file_path}")
        
        return msg
    
    def check_gmail_block(self, error_message):
        """Check if the error might be related to Gmail's temporary block"""
        block_indicators = [
            "temporary disable",
            "unusual activity",
            "unusual sign",
            "unusual attempt",
            "temporarily locked",
            "temporary lock",
            "account has been disabled",
            "account was disabled",
            "try again later"
        ]
        
        error_str = str(error_message).lower()
        for indicator in block_indicators:
            if indicator in error_str:
                return True
        return False
    
    def send_email(self, msg, recipient_email, cc=None, bcc=None):
        if not self.session:
            if not self.connect():
                return False
        
        recipients = [recipient_email]
        if cc:
            recipients.extend(cc)
        if bcc:
            recipients.extend(bcc)
        
        try:
            self.session.send_message(msg, self.sender_email, recipients)
            logging.info(f"Email sent to {recipient_email}")
            return True
        except Exception as e:
            if self.check_gmail_block(e):
                logging.error(f"Gmail block detected: {e}")
                logging.error("Pausing for 1 hour to avoid account suspension")
                # Save state to resume later
                return "BLOCKED"
            logging.error(f"Failed to send email to {recipient_email}: {e}")
            return False
    
    def send_bulk_emails(self, csv_file, subject, html_template, text_template=None, attachment_paths=None, 
                         personalize=True, delay_base=1, max_emails_per_day=450, 
                         batch_size=20, resume_from=0, save_state=True):
        """
        Send bulk emails with more human-like patterns and throttling
        
        Parameters:
        - batch_size: Number of emails to send before taking a longer break
        - resume_from: Index to resume sending from (for continuing interrupted campaigns)
        - save_state: Whether to save progress state for resuming later
        """
        if not self.session:
            if not self.connect():
                return False
                
        results = {"success": 0, "failed": 0, "skipped": 0}
        try:
            df = pd.read_csv(csv_file)    
            
            if "Emails" not in df.columns:
                logging.error("Invalid csv file, must contain an `Emails` column")
                return False
            
            df = df.dropna(subset=["Emails"])
            
            # If resuming, skip already processed records
            if resume_from > 0:
                if resume_from >= len(df):
                    logging.error(f"Resume index {resume_from} is greater than the number of records {len(df)}")
                    return False
                logging.info(f"Resuming campaign from record {resume_from}")
                skipped_records = df.iloc[:resume_from]
                results["skipped"] = len(skipped_records)
                df = df.iloc[resume_from:]
            
            if len(df) > max_emails_per_day:
                logging.warning(f"Limiting to {max_emails_per_day} emails per day, due to Gmail's daily sending limit")
                df = df.head(max_emails_per_day)
            
            total_recipients = len(df)
            start_time = datetime.now()
            logging.info(f"Starting bulk email campaign to {total_recipients} recipients")
            
            # Create a state file for resuming
            state_file = f"email_campaign_state_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
            
            for index, row in df.iterrows():
                real_index = index + resume_from
                recipient_email = row["Emails"].strip()
                
                # Personalize content if needed
                if personalize:
                    personalized_html = html_template
                    personalized_text = text_template if text_template else None
                    
                    # Replace placeholders with recipient data
                    for key, value in row.items():
                        placeholder = f"{{{{{key}}}}}"
                        # Handle NaN values
                        str_value = "" if pd.isna(value) else str(value)
                        personalized_html = personalized_html.replace(placeholder, str_value)
                        if personalized_text:
                            personalized_text = personalized_text.replace(placeholder, str_value)
                else:
                    personalized_html = html_template
                    personalized_text = text_template
                
                # Process CC and BCC if they exist in the dataframe
                cc = row["cc"]
                
                bcc = None
                
                # Create and send the email
                msg = self.create_message(
                    recipient_email=recipient_email,
                    subject=subject,
                    html_content=personalized_html,
                    text_content=personalized_text,
                    attachment_paths=attachment_paths,
                    cc=cc,
                    bcc=bcc
                )  
                
                # Show progress for every email
                progress = f"Processing email {index + 1}/{total_recipients} ({(index + 1)/total_recipients*100:.1f}%)"
                print(f"\r{progress}", end="", flush=True)
                
                result = self.send_email(msg, recipient_email, cc, bcc)
                if result == "BLOCKED":
                    # Gmail block detected - save state and exit
                    if save_state:
                        self._save_campaign_state(state_file, real_index + 1, results)
                    
                    logging.warning(f"Campaign paused due to Gmail restrictions. Resume later with --resume {real_index + 1}")
                    return results
                
                if result:
                    results["success"] += 1
                    logging.info(f"✓ Sent to {recipient_email}")
                else:
                    results["failed"] += 1
                    logging.error(f"✗ Failed to send to {recipient_email}")
                
                # Add randomized delay to mimic human behavior
                current_delay = delay_base + random.uniform(0.5, 1.5)
                if index < total_recipients - 1:
                    time.sleep(current_delay)
                
                # Take a longer break after each batch
                if (index + 1) % batch_size == 0:
                    logging.info(f"Taking a longer break after {batch_size} emails")
                    time.sleep(random.uniform(300, 600))  # 5 to 10 minutes break
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logging.info(f"Bulk email campaign completed in {duration:.2f} seconds")
            logging.info(f"Results: {results['success']} successful, {results['failed']} failed, {results['skipped']} skipped")
            
            # Print a newline after the progress bar is done
            print()
            
            summary = (
                f"\nBulk email campaign completed:"
                f"\n- Duration: {duration:.2f} seconds"
                f"\n- Successful: {results['success']}"
                f"\n- Failed: {results['failed']}"
                f"\n- Skipped: {results['skipped']}"
            )
            logging.info(summary)
            
        except Exception as e:
            logging.error(f"Error in buld email process: {str(e)}")
            return False
        finally:
            self.disconnect()
        
        return results
    
    def _save_campaign_state(self, state_file, resume_index, results):
        """Save the current state of the campaign to a file"""
        state = {
            "resume_index": resume_index,
            "results": results
        }
        with open(state_file, 'w') as f:
            json.dump(state, f)
        logging.info(f"Campaign state saved to {state_file}")


def load_email_template(template_path):
    try:
        with open(template_path, 'r', encoding='utf-8') as file:
            return file.read()
    except Exception as e:
        logging.error(f"Failed to load template: {e}")
        return None

def load_environment(env_name):
    """Load environment variables from the specified .env file"""
    os.environ.clear()
    env_file = f".env.{env_name}"
    if not os.path.exists(env_file):
        print(f"Error: Environment file '{env_file}' does not exist")
        print("Available environments: it, pension, governance")
        exit(1)
    
    load_dotenv(env_file)
    print(f"Loaded environment: {env_name}")
    print(f"Sender: {os.getenv('SENDER_EMAIL')} ({os.getenv('SENDER_NAME')})")

# Set up command line argument parsing
parser = argparse.ArgumentParser(description='Bulk Email Sender')
parser.add_argument('-e', '--env', 
                    choices=['it', 'pension', 'governance'],
                    default='it',
                    help='Select the environment to use (default: it)')
parser.add_argument('--resume', type=int, default=0, help='Resume campaign from a specific index')

# Parse arguments
args = parser.parse_args()
# Load the selected environment
load_environment(args.env)

if __name__ == "__main__":
    subject = "Invitation: Women in Corporate & Business Dinner – March 14th, 2025"
    
    csv = None
    
    if args.env == 'pension':
        # csv = "prospects-email-cleaned.csv"
        csv = "prospects-email-cleaned-2.csv"
    elif args.env == 'governance':
        # csv = "governance-email-cleaned.csv"
        csv = "governance-email-cleaned-2.csv"
    elif args.env == 'it':
        csv = "testing.csv"
    else:
        print("Invalid environment")
        exit(1)
        
    html_template = load_email_template("email-templates/women-in-business/women-in-business.html")
    
    text_template = load_email_template("email-templates/women-in-business/women-in-business.txt")
    
    attachments = [
        "assets/company_profile.pdf",
        "assets/Ascent_Calendar_2025.pdf",
        "assets/women-in-business-poster.jpeg"
    ]
    
    sender = BulkEmailSender(
        smtp_server='smtp.gmail.com',
        smtp_port=587,
        username=os.environ.get('EMAIL_USERNAME'),
        password=os.environ.get('EMAIL_PASSWORD'),
        sender_email=os.environ.get('SENDER_EMAIL'),
        sender_name=os.environ.get('SENDER_NAME'),
    )
    
    results = sender.send_bulk_emails(
        csv_file=csv,
        subject=subject,
        html_template=html_template,
        text_template=text_template,
        attachment_paths=attachments,
        personalize=True,
        delay_base=6,
        max_emails_per_day=500,
        resume_from=args.resume
    )
    
    print(f"Email campaign summary: {results}")
