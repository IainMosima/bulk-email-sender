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
    
    def connect(self):
        try:
            self.session = smtplib.SMTP(self.smtp_server, self.smtp_port)
            self.session.ehlo()
            self.session.starttls() # enable encryption
            self.session.ehlo()
            self.session.login(self.username, self.password)
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
            # msg["Cc"] = ", ".join(cc)
            msg["Cc"] = cc
        if bcc:
            # msg["Bcc"] = ", ".join(bcc)
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
            logging.error(f"Failed to send email to {recipient_email}: {e}")
            return False
    
    def send_bulk_emails(self, csv_file, subject, html_template, text_template=None, attachment_paths=None, personalize=True, delay_base=1, max_emails_per_day=450):
        if not self.session:
            if not self.connect():
                return False
        results = {"success": 0, "failed": 0, "skipped": 0}
        try:
            df = pd.read_csv(csv_file)    
            
            if "Emails" not in df.columns:
                logging.error("Invalid csv file, must contain an `Emails` column")
            
            df = df.dropna(subset=["Emails"])
            
            if len(df) > max_emails_per_day:
                logging.warning(f"Limiting to {max_emails_per_day} emails per day, due to Gmail's daily sending limit")
                df = df.head(max_emails_per_day)
            
            total_recipients = len(df)
            start_time = datetime.now()
            logging.info(f"Starting bulk email campaign to {total_recipients} recipients")
            
            for index, row in df.iterrows():
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
                # if 'cc' in row and not pd.isna(row['cc']):
                #     cc = [email.strip() for email in str(row['cc']).split(',')] if ',' in str(row['cc']) else row['cc']
                
                bcc = None
                # if 'bcc' in row and not pd.isna(row['bcc']):
                #     bcc = [email.strip() for email in str(row['bcc']).split(',')] if ',' in str(row['bcc']) else row['bcc']
                
                
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
                
                if self.send_email(msg, recipient_email, cc, bcc):
                    results["success"] += 1
                    logging.info(f"✓ Sent to {recipient_email}")
                else:
                    results["failed"] += 1
                    logging.error(f"✗ Failed to send to {recipient_email}")
                
                # Remove the progress reporting every 10 emails since we're showing it for each one
                # if (index + 1) % 10 == 0:
                #     logging.info(f"Progress: {index + 1}/{total_recipients} emails sent")
                
                # Add 0.5 secondd for every 50 emails
                current_delay = delay_base + (index // 50) * 0.5
                if current_delay > 6:  # Cap at 10 seconds
                    current_delay = 6
                    
                if index < total_recipients - 1:
                    time.sleep(current_delay)
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                logging.info(f"Bulk email campaign completed in {duration:.2f} seconds")
                logging.info(f"Results: {results['success']} successful, {results['failed']} failed, {results['skipped']} skipped")
            
            # Print a newline after the progress bar is done
            print()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
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


def load_email_template(template_path):
    try:
        with open(template_path, 'r', encoding='utf-8') as file:
            return file.read()
    except Exception as e:
        logging.error(f"Failed to load template: {e}")
        return None

if __name__ == "__main__":
    os.environ.clear()
    dotenv.load_dotenv()
    # TODO: change here
    # csv = "prospects-email-cleaned.csv"
    # csv = "testing.csv"
    csv = "governance-email-cleaned.csv"
    
    html_template = load_email_template("email-templates/Advanced-Records-Management.html")
    
    text_template = load_email_template("email-templates/Advanced-Records-Management.txt")
    
    attachments = [
        "assets/company_profile.pdf",
        "assets/Ascent_Calendar_2025.pdf",
        "assets/advanced-record-management/advanced records management & digital transformation workshop.jpeg"
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
        subject='Advanced Records Management & Digital Transformation',
        html_template=html_template,
        text_template=text_template,
        attachment_paths=attachments,
        personalize=True,
        delay_base=6,
        max_emails_per_day=500
    )
    
    print(f"Email campaign summary: {results}")
