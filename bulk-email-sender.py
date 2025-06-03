import argparse
import json
import logging
import os
import random
import smtplib
import time
import re
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from dotenv import load_dotenv

# Set up logging
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler('bulk-email-sender.log')
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)
logger.addHandler(file_handler)


class BulkEmailSender:
    def __init__(self, smtp_server, smtp_port, username, password, sender_email):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.sender_email = sender_email
        self.session = None
        self.connection_attempts = 0
        self.max_connection_attempts = 3
        self.retry_delay = 60

    def connect(self):
        try:
            self.connection_attempts += 1
            self.session = smtplib.SMTP(self.smtp_server, self.smtp_port)
            self.session.ehlo()
            self.session.starttls()
            self.session.ehlo()
            self.session.login(self.username, self.password)
            self.connection_attempts = 0
            logging.info("Successfully connected to SMTP server")
            return True
        except Exception as e:
            logging.error(f"Failed to connect to SMTP server: {e}")
            return False

    def disconnect(self):
        if self.session:
            self.session.quit()
            self.session = None
            logging.info("Disconnected from SMTP server")

    def _is_valid_email(self, email):
        """Basic email validation"""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(email_pattern, email))

    def create_message(self, recipient_email, subject, html_content, text_content=None, attachment_paths=None, cc=None,
                       bcc=None):
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.sender_email
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
                self.session = None
                return "BLOCKED"
            if isinstance(e, (smtplib.SMTPServerDisconnected, OSError, ConnectionError)):
                logging.warning(f"Connection lost to {recipient_email}: {e}")
                self.session = None
                return False
            logging.error(f"Failed to send email to {recipient_email}: {e}")
            return False

    def send_bulk_emails(self, csv_file, subject, html_template, text_template=None, attachment_paths=None,
                         personalize=True, delay_base=1, max_emails_per_day=450,
                         batch_size=20, resume_from=0, save_state=True):
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

            state_file = f"email_campaign_state_{start_time.strftime('%Y%m%d_%H%M%S')}.json"

            for index, row in df.iterrows():
                real_index = index + resume_from
                recipient_email = row["Emails"].strip()

                cc = None
                if "cc" in row and pd.notna(row["cc"]):
                    cc = [email.strip() for email in str(row["cc"]).split(",") if email.strip()]
                    cc = [email for email in cc if self._is_valid_email(email)]
                    if not cc:
                        cc = None
                        logging.warning(f"No valid CC emails for recipient {recipient_email}")

                bcc = None
                if "bcc" in row and pd.notna(row["bcc"]):
                    bcc = [email.strip() for email in str(row["bcc"]).split(",") if email.strip()]
                    bcc = [email for email in bcc if self._is_valid_email(email)]
                    if not bcc:
                        bcc = None
                        logging.warning(f"No valid BCC emails for recipient {recipient_email}")

                if personalize:
                    personalized_html = html_template
                    personalized_text = text_template if text_template else None
                    for key, value in row.items():
                        placeholder = f"{{{{{key}}}}}"
                        str_value = "" if pd.isna(value) else str(value)
                        personalized_html = personalized_html.replace(placeholder, str_value)
                        if personalized_text:
                            personalized_text = personalized_text.replace(placeholder, str_value)
                else:
                    personalized_html = html_template
                    personalized_text = text_template

                msg = self.create_message(
                    recipient_email=recipient_email,
                    subject=subject,
                    html_content=personalized_html,
                    text_content=personalized_text,
                    attachment_paths=attachment_paths,
                    cc=",".join(cc) if cc else None,
                    bcc=",".join(bcc) if bcc else None
                )

                progress = f"Processing email {index + 1}/{total_recipients} ({(index + 1) / total_recipients * 100:.1f}%)"
                print(f"\r{progress}", end="", flush=True)

                result = self.send_email(msg, recipient_email, cc, bcc)
                if result == "BLOCKED":
                    if save_state:
                        self._save_campaign_state(state_file, real_index + 1, results)
                    logging.warning(
                        f"Campaign paused due to Gmail restrictions. Resume later with --resume {real_index + 1}")
                    return results

                if result:
                    results["success"] += 1
                    logging.info(f"✓ Sent to {recipient_email}")
                else:
                    results["failed"] += 1
                    logging.error(f"✗ Failed to send to {recipient_email}")

                current_delay = delay_base + random.uniform(5, 19)
                if index < total_recipients - 1:
                    time.sleep(current_delay)

                if (index + 1) % batch_size == 0:
                    logging.info(f"Taking a longer break after {batch_size} emails")
                    self.disconnect()
                    time.sleep(random.uniform(300, 600))
                    if not self.connect():
                        logging.error("Failed to reconnect to SMTP server after break")
                        return results

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logging.info(f"Bulk email campaign completed in {duration:.2f} seconds")
            logging.info(
                f"Results: {results['success']} successful, {results['failed']} failed, {results['skipped']} skipped")

            print("\n")
            summary = (
                f"\nBulk email campaign completed:"
                f"\n- Duration: {duration:.2f} seconds"
                f"\n- Successful: {results['success']}"
                f"\n- Failed: {results['failed']}"
                f"\n- Skipped: {results['skipped']}"
            )
            logging.info(summary)

        except Exception as e:
            logging.error(f"Error in bulk email process: {str(e)}")
            return False
        finally:
            self.disconnect()

        return results

    def _save_campaign_state(self, state_file, resume_index, results):
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
    os.environ.clear()
    env_file = f".env.{env_name}"
    if not os.path.exists(env_file):
        print(f"Error: Environment file '{env_file}' does not exist")
        print("Available environments: it, pension, governance")
        exit(1)
    load_dotenv(env_file)
    print(f"Loaded environment: {env_name}")
    print(f"Sender: {os.getenv('SENDER_EMAIL')}")


parser = argparse.ArgumentParser(description='Bulk Email Sender')
parser.add_argument('-e', '--env', choices=['it', 'pension', 'governance'], default='it',
                    help='Select the environment to use (default: it)')
parser.add_argument('--resume', type=int, default=0, help='Resume campaign from a specific index')

args = parser.parse_args()
load_environment(args.env)


def email_sender(subject, files, custom_html_template, custom_text_template):
    sender = BulkEmailSender(
        smtp_server='smtp.gmail.com',
        smtp_port=587,
        username=os.environ.get('EMAIL_USERNAME'),
        password=os.environ.get('EMAIL_PASSWORD'),
        sender_email=os.environ.get('SENDER_EMAIL'),
    )

    final_results = sender.send_bulk_emails(
        csv_file=csv,
        subject=subject,
        html_template=custom_html_template,
        text_template=custom_text_template,
        attachment_paths=files,
        personalize=True,
        delay_base=6,
        max_emails_per_day=500,
        resume_from=args.resume
    )

    return final_results


if __name__ == "__main__":
    csv = None
    if args.env == 'pension':
        csv = "emails/june/june-3/pension.csv"
    elif args.env == 'governance':
        csv = "emails/june/june-3/governace.csv"
    elif args.env == 'it':
        # csv = "emails/june/june-3/it.csv"
        csv = "testing.csv"
    else:
        print("Invalid environment")
        exit(1)

    if args.env == 'pension':
        subject_pension = "RE: Master Customer Service Excellence - Transform Your Customer Experience | June 23RD -27TH, 2025"
        html_template = load_email_template("email-templates/to-send.html")
        text_template = load_email_template("email-templates/to-send.txt")
        attachments = [
            "assets/june-3/Masterclass in Executive office management & administration.pdf",
            "assets/june-3/Nomination form - Effective office management and administration.pdf"
        ]
        results = email_sender(subject_pension, attachments, html_template, text_template)
    elif args.env == 'governance':
        subject_governance = "RE: Master Customer Service Excellence - Transform Your Customer Experience | June 23RD -27TH, 2025"
        html_template = load_email_template("email-templates/to-send.html")
        text_template = load_email_template("email-templates/to-send.txt")
        attachments = [
            "assets/june-3/Masterclass in Executive office management & administration.pdf",
            "assets/june-3/Nomination form - Effective office management and administration.pdf"
        ]
        results = email_sender(subject_governance, attachments, html_template, text_template)
    else:
        subject_it = "RE: Master Customer Service Excellence - Transform Your Customer Experience | June 23RD -27TH, 2025"
        html_template = load_email_template("email-templates/to-send.html")
        text_template = load_email_template("email-templates/to-send.txt")
        attachments = [
            "assets/june-3/Masterclass in Executive office management & administration.pdf",
            "assets/june-3/Nomination form - Effective office management and administration.pdf"
        ]
        results = email_sender(subject_it, attachments, html_template, text_template)

    print(f"Email campaign summary: {results}")
