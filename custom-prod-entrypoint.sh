#!/bin/bash

set -e
set -x

set -o allexport
source .env-base
set +o allexport

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${POSTGRESQL_ADDON_URI//\"/}"

airflow db migrate

if [[ "$CELLAR_ADDON_HOST" != "" ]]; then
    export AIRFLOW_CONN_LOG_CONNECTION="aws:///?__extra__=%7B%22endpoint_url%22%3A+%22https%3A%2F%2F${CELLAR_ADDON_HOST//\"/}%22%2C+%22aws_access_key_id%22%3A+%22${CELLAR_ADDON_KEY_ID//\"/}%22%2C+%22aws_secret_access_key%22%3A+%22${CELLAR_ADDON_KEY_SECRET//\"/}%22%2C+%22config_kwargs%22%3A+%7B%22request_checksum_calculation%22%3A+%22when_required%22%2C+%22response_checksum_validation%22%3A+%22when_required%22%7D%7D"
    export AIRFLOW__LOGGING__REMOTE_LOGGING=True
    export AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER="s3://${LOG_BUCKET_NAME//\"/}/logs"
    export AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID="log_connection"
    export AIRFLOW__LOGGING__ENCRYPT_S3_LOGS=False
fi

# shellcheck disable=SC2153
if [[ "$AIRFLOW__SENTRY__SENTRY_DSN" != "" ]]; then
    export AIRFLOW__SENTRY__SENTRY_ON=True
fi


if [[ "$AIRFLOW_SUPERUSER_PASSWORD" != "" ]]; then
    airflow users create \
        --role Admin \
        --email airflow-admin@inclusion.beta.gouv.fr \
        --firstname admin \
        --lastname admin \
        --username admin \
        --password "${AIRFLOW_SUPERUSER_PASSWORD}"
fi

exec /entrypoint "standalone"
