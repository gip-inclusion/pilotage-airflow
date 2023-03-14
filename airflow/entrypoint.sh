#!/bin/bash

set -e
set -x

BASE_DIR=$(pwd)

export AIRFLOW_HOME="${BASE_DIR}/airflow"

AIRFLOW_DATABASE_URL="${AIRFLOW_DATABASE_URL:-$POSTGRESQL_ADDON_URI}"

export AIRFLOW__API__AUTH_BACKENDS="airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Europe/Paris
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export SQLALCHEMY_SILENCE_UBER_WARNING=1

export AIRFLOW__CORE__DAGS_FOLDER="${BASE_DIR}/dags"
export AIRFLOW__CORE__FERNET_KEY="${SECRET_KEY}"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${AIRFLOW_DATABASE_URL}"

export DBT_LOG_PATH="/tmp/dbt-logs"
export DBT_TARGET_PATH="/tmp/dbt-target"

airflow db upgrade

airflow users create \
    --role Admin \
    --email airflow-admin@inclusion.beta.gouv.fr \
    --firstname admin \
    --lastname admin \
    --username admin \
    --password "${AIRFLOW_SUPERUSER_PASSWORD}"

airflow scheduler &

airflow webserver --port 8080
