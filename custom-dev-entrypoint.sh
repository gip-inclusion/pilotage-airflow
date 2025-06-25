#!/bin/bash

set -e
set -x

set -o allexport
source .env-base
set +o allexport

airflow db migrate
airflow variables import dag-variables.json

airflow users create \
    --role Admin \
    --email admin@example.com \
    --firstname admin \
    --lastname admin \
    --username admin \
    --password password

# Create buckets. Use `|| true` because we get an error : 409 (BucketAlreadyOwnedByYou)
s3cmd --host="http://minio:9000" --host-bucket="http://minio:9000" --no-ssl --access_key="minioadmin" --secret_key="minioadmin" mb s3://airflow || true
s3cmd --host="http://minio:9000" --host-bucket="http://minio:9000" --no-ssl --access_key="minioadmin" --secret_key="minioadmin" mb s3://les-emplois || true

exec /entrypoint "standalone"
