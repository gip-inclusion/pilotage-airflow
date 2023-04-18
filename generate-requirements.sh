#!/bin/bash

# FIXME(vperron): this file is managed manually for now,
# and applies every dependency one by one. Airflow
# is difficult to install with pip's dependency resolver
# and is even harder alongside DBT since there are dependency
# conflicts. Until this is resolved, stick to this.

AIRFLOW_VERSION=2.5.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install apache-airflow[amazon,sentry,slack]
pip install dbt-postgres==1.4.5
pip install pandas
pip install black
pip install flake8
pip install isort
pip install sqlfluff
pip install sqlfluff-templater-dbt

pip freeze > requirements-ci.txt
