import boto3
from airflow.models import Variable
from botocore.client import Config


def client():
    return boto3.client(
        "s3",
        endpoint_url=Variable.get("DATASTORE_S3_ENDPOINT_URL"),
        aws_access_key_id=Variable.get("DATASTORE_S3_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("DATASTORE_S3_SECRET_KEY"),
        config=Config(signature_version="s3v4"),
    )
