#!/bin/bash

set -e
set -x

source _source-vars.sh

airflow db upgrade

if [[ "x$CELLAR_ADDON_HOST" != "x" ]]; then
    export AIRFLOW_CONN_LOG_CONNECTION="aws://@/?endpoint_url=https%3A%2F%2F${CELLAR_ADDON_HOST//\"/}&region_name=eu-west-1&aws_access_key_id=${CELLAR_ADDON_KEY_ID//\"/}&aws_secret_access_key=${CELLAR_ADDON_KEY_SECRET//\"/}"
    export AIRFLOW__LOGGING__REMOTE_LOGGING=True
    export AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER="s3://${LOG_BUCKET_NAME//\"/}/logs"
    export AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID="log_connection"
    export AIRFLOW__LOGGING__ENCRYPT_S3_LOGS=False
fi


if [[ "x$SENTRY_DSN" != "x" ]]; then
    export AIRFLOW__SENTRY__SENTRY_ON=True
    export AIRFLOW__SENTRY__SENTRY_DSN="${SENTRY_DSN}"
fi


if [[ "x$AIRFLOW_SUPERUSER_PASSWORD" != "x" ]]; then
    airflow users create \
        --role Admin \
        --email airflow-admin@inclusion.beta.gouv.fr \
        --firstname admin \
        --lastname admin \
        --username admin \
        --password "${AIRFLOW_SUPERUSER_PASSWORD}"
fi

airflow scheduler &

airflow webserver --port 8080
