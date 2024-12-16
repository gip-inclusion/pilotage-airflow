import os

import boto3
from botocore.client import Config


def client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("ASP_S3_HOST"),
        aws_access_key_id=os.getenv("ASP_S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("ASP_S3_SECRET_KEY"),
        config=Config(signature_version="s3v4"),
    )
