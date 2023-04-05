
BASE_DIR="${AIRFLOW_BASE_DIR:-$(pwd)}"

export AIRFLOW_BASE_DIR="${BASE_DIR}"

export AIRFLOW_HOME="${BASE_DIR}/airflow"
export PYTHONPATH="${PYTHONPATH}:${BASE_DIR}"  # be able to resolve "import dags"

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
