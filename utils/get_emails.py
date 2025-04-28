def get_emails(emails_column):
    def email_extractor(emails_info):
        if len(emails_info.split(" /")) > 1:
            # print(emails_info.split(" /"))
            return emails_info.split(" /")
        elif len(emails_info.split("/")) > 1:
            return emails_info.split("/")
        elif len(emails_info.split(",")) > 1:
            return emails_info.split(",")
        elif len(emails_info.split(", ")) > 1:
            return emails_info.split(", ")

        return emails_info

    result = []

    for column in emails_column:
        email = email_extractor(column)

        if type(email) == list:
            result.extend(email)
        else:
            result.append(email)
    return result
