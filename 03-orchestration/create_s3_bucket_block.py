import os

from dotenv import load_dotenv
from prefect_aws import AwsCredentials, S3Bucket


def create_aws_creds_block():
    load_dotenv("src/.env")

    aws_creds_block = AwsCredentials(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    aws_creds_block.save(name="aws-credentials", overwrite=True)


def create_s3_bucket_block():
    aws_creds_block = AwsCredentials.load("aws-credentials")
    s3_bucket_block = S3Bucket(
        bucket_name="nyc-taxi-trips-data", credentials=aws_creds_block
    )
    s3_bucket_block.save(name="aws-s3", overwrite=True)


if __name__ == "__main__":
    create_aws_creds_block()
    create_s3_bucket_block()
