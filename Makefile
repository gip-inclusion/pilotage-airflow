# Delete target on error.
# https://www.gnu.org/software/make/manual/html_node/Errors.html#Errors
# > This is almost always what you want make to do, but it is not historical
# > practice; so for compatibility, you must explicitly request it
.DELETE_ON_ERROR:

PYTHON_VERSION := python3.10

VIRTUAL_ENV ?= $(shell pwd)/.venv
export PATH := $(VIRTUAL_ENV)/bin:$(PATH)

DUMP_DIR ?= .latest.dump

MONITORED_DIRS := dags tests

# FIXME(vperron): In the long run we should include "references.consistent,references.from,references.qualification" rules.
SQLFLUFF_OPTIONS := \
	--exclude-rules ambiguous.distinct,layout.long_lines,references.consistent,references.from,references.qualification \
	--disable-progress-bar --nocolor

.PHONY: venv_ci clean compile-deps airflow_init dbt_clean dbt_docs fix quality test load_dump

venv_ci:
	$(PYTHON_VERSION) -m venv $(VIRTUAL_ENV)
	$(VIRTUAL_ENV)/bin/pip install --no-color --progress-bar off -r requirements-ci.txt

dbt_clean:
	rm -rf $(DBT_TARGET_PATH)
	dbt clean

dbt_docs:
	dbt docs generate

clean: dbt_clean
	find . -depth -type d -name "__pycache__" -exec rm -rf '{}' \;
	find . -type d -empty -delete

# if `sqlfluff fix` does not work, use `sqlfluff parse` to investigate.
fix: clean
	black $(MONITORED_DIRS)
	isort $(MONITORED_DIRS)
	sqlfluff fix --force $(SQLFLUFF_OPTIONS) $(MONITORED_DIRS)

quality: clean
	black --check $(MONITORED_DIRS)
	isort --check $(MONITORED_DIRS)
	flake8 --count --show-source --statistics $(MONITORED_DIRS)
	sqlfluff lint $(SQLFLUFF_OPTIONS) $(MONITORED_DIRS)

airflow_initdb:
	createdb airflow || true
	airflow db reset -y && \
		airflow db upgrade && \
		airflow variables import dag-variables.json && \
		airflow users create --role Admin --email admin@example.com --username admin --password password -f "" -l ""

test:
	pytest -v -W ignore::DeprecationWarning

load_dump:
	dropdb ${PGDATABASE} || true
	createdb ${PGDATABASE}
	rm -rf $(DUMP_DIR)
	PGDATABASE=${PROD_PGDATABASE} PGHOST=${PROD_PGHOST} PGPORT=${PROD_PGPORT} PGUSER=${PROD_PGUSER} PGPASSWORD=${PROD_PGPASSWORD} \
		   pg_dump --format=directory --no-owner --no-acl --verbose --jobs=4 \
		   --exclude-table="lh_*" --exclude-table="yp_*" --exclude-table="z_*" \
		   --file=$(DUMP_DIR)
	pg_restore --format=directory --clean --jobs=4 --verbose --no-owner --no-acl -d ${PGDATABASE} $(DUMP_DIR) || true
