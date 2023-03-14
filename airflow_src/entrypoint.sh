#!/bin/bash

set -e
set -x

BASE_DIR="${AIRFLOW_BASE_DIR:-$(pwd)}"

export AIRFLOW_HOME="${BASE_DIR}/airflow"

AIRFLOW_DATABASE_URL="${AIRFLOW_DATABASE_URL:-$POSTGRESQL_ADDON_URI}"

export AIRFLOW__API__AUTH_BACKENDS="airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Europe/Paris
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
export AIRFLOW__CORE__LOAD_EXAMPLES=False

export SQLALCHEMY_SILENCE_UBER_WARNING=1

export AIRFLOW__CORE__DAGS_FOLDER="${BASE_DIR}/dags"

# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
export AIRFLOW__CORE__FERNET_KEY="${SECRET_KEY}"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${AIRFLOW_DATABASE_URL}"
# cf https://github.com/apache/airflow/issues/17536#issuecomment-900343494
# without this, any login redirects us to HTTP
export AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=True

export DBT_LOG_PATH="/tmp/dbt-logs"
export DBT_TARGET_PATH="/tmp/dbt-target"

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
