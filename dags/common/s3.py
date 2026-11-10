import boto3
from airflow.sdk import Variable
from botocore.client import Config


def client(prefix="DATASTORE_S3"):
    return boto3.client(
        "s3",
        endpoint_url=Variable.get(f"{prefix}_ENDPOINT_URL"),
        aws_access_key_id=Variable.get(f"{prefix}_ACCESS_KEY"),
        aws_secret_access_key=Variable.get(f"{prefix}_SECRET_KEY"),
        config=Config(signature_version="s3v4"),
    )
