import re


def get_emails(emails):
    def split_by_separators(emails_info):
        if not isinstance(emails_info, str):
            return [emails_info]

        # Define potential separators and try them in order
        separators = [" /", "/", ", ", ",", "or ", "or", "|", " | "]

        for separator in separators:
            if separator in emails_info:
                return [item.strip() for item in emails_info.split(separator)]

        # If no separators found, return the original as a single item
        return [emails_info.strip()]

    def is_valid_email(text):
        if not isinstance(text, str):
            return False

        # Basic email validation pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        text = text.strip()
        return bool(re.match(email_pattern, text))

    result = []

    for item in emails:
        # First split the input by potential separators
        email_candidates = split_by_separators(item)

        # Then filter for valid emails
        for candidate in email_candidates:
            if is_valid_email(candidate):
                result.append(candidate)

    return result