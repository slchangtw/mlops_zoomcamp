import os

from dotenv import load_dotenv
from prefect_email import EmailServerCredentials


def create_email_server_credentials_block():
    load_dotenv("src/.env")

    credentials = EmailServerCredentials(
        username=os.getenv("EMAIL_ADDRESS"),
        password=os.getenv("EMAIL_PASSWORD"),
    )
    credentials.save("email-server-credentials-block", overwrite=True)


if __name__ == "__main__":
    create_email_server_credentials_block()
