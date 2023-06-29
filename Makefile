# Delete target on error.
# https://www.gnu.org/software/make/manual/html_node/Errors.html#Errors
# > This is almost always what you want make to do, but it is not historical
# > practice; so for compatibility, you must explicitly request it
.DELETE_ON_ERROR:

PYTHON_VERSION := python3.10

VIRTUAL_ENV ?= $(shell pwd)/.venv
export PATH := $(VIRTUAL_ENV)/bin:$(PATH)

DUMP_DIR ?= .latest.dump
RESTORE_DIR ?= .latest.restore

MONITORED_DIRS := dags dbt tests

# FIXME(vperron): In the long run we should include "references.consistent,references.from,references.qualification" rules.
SQLFLUFF_OPTIONS := \
	--exclude-rules ambiguous.distinct,layout.long_lines,references.consistent,references.from,references.qualification,references.special_chars \
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
fix:
	black $(MONITORED_DIRS)
	ruff check --fix $(MONITORED_DIRS)
	sqlfluff fix --force $(SQLFLUFF_OPTIONS) $(MONITORED_DIRS)

quality:
	black --check $(MONITORED_DIRS)
	ruff check $(MONITORED_DIRS)
	sqlfluff lint $(SQLFLUFF_OPTIONS) $(MONITORED_DIRS)

airflow_initdb:
	createdb airflow || true
	airflow db reset -y && \
		airflow db upgrade && \
		airflow variables import dag-variables.json && \
		airflow users create --role Admin --email admin@example.com --username admin --password password -f "" -l ""

test:
	pytest -v -W ignore::DeprecationWarning


pg_dump:
	@PGDATABASE=${PROD_PGDATABASE} PGHOST=${PROD_PGHOST} PGPORT=${PROD_PGPORT} PGUSER=${PROD_PGUSER} PGPASSWORD=${PROD_PGPASSWORD} \
			psql -c ";" || { \
				echo "\n\n### Your connexion to the production database failed. Is the port blocked by a firewall ? ###\n" ; \
				false ; \
			}
	rm -rf $(DUMP_DIR)
	@echo "\n\n### Dumping the production database on your machine, check your disk size and connection speeds ! ###\n"
	PGDATABASE=${PROD_PGDATABASE} PGHOST=${PROD_PGHOST} PGPORT=${PROD_PGPORT} PGUSER=${PROD_PGUSER} PGPASSWORD=${PROD_PGPASSWORD} \
		   time pg_dump --format=directory --no-owner --no-acl --verbose --jobs=4 \
		   --exclude-table="lh_*" --exclude-table="yp_*" --exclude-table="z_*" \
		   --file=$(DUMP_DIR)
	@echo "\n\n### Database dumped successfully. ###\n"
	rm -rf $(RESTORE_DIR)
	mv $(DUMP_DIR) $(RESTORE_DIR)

pg_restore:
ifneq (,$(findstring clever-cloud,$(PGHOST)))
	@echo "\n\n### Your environment seems to connect directly to the production database, aborting. ###\n"
else
	dropdb ${PGDATABASE} || true
	createdb ${PGDATABASE}
	time pg_restore --format=directory --clean --jobs=4 --verbose --no-owner --no-acl -d ${PGDATABASE} $(RESTORE_DIR) || true
	@echo "\n\n### Database restored successfully ! ###\n"
endif

load_dump: pg_dump pg_restore
