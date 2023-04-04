#!/bin/bash

set -e
set -x

source _source-vars.sh

if [[ "x$AIRFLOW_SUPERUSER_PASSWORD" != "x" ]]; then
    airflow users create \
        --role Admin \
        --email airflow-admin@inclusion.beta.gouv.fr \
        --firstname admin \
        --lastname admin \
        --username admin \
        --password "${AIRFLOW_SUPERUSER_PASSWORD}"
fi

airflow webserver --port 8080
