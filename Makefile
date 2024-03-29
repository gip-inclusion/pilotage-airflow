# Delete target on error.
# https://www.gnu.org/software/make/manual/html_node/Errors.html#Errors
# > This is almost always what you want make to do, but it is not historical
# > practice; so for compatibility, you must explicitly request it
.DELETE_ON_ERROR:

VIRTUAL_ENV ?= $(shell pwd)/.venv
export PATH := $(VIRTUAL_ENV)/bin:$(PATH)


# Python
# =============================================================================
.PHONY: venv_ci update

PYTHON_VERSION := python3.10

venv_ci:
	$(PYTHON_VERSION) -m venv $(VIRTUAL_ENV)

update:
	$(VIRTUAL_ENV)/bin/pip install --force-reinstall --no-color --progress-bar off -r requirements-ci.txt
	$(VIRTUAL_ENV)/bin/pip install --force-reinstall --no-color --progress-bar off dbt-fal==1.5.4 dbt-core==1.5.1


# Quality
# =============================================================================
.PHONY: fix quality test clean

MONITORED_DIRS := dags dbt tests
# FIXME(vperron): In the long run we should include "references.consistent,references.from,references.qualification" rules.
SQLFLUFF_OPTIONS := \
	--exclude-rules ambiguous.distinct,layout.long_lines,references.consistent,references.from,references.qualification,references.special_chars \
	--disable-progress-bar --nocolor

# if `sqlfluff fix` does not work, use `sqlfluff parse` to investigate.
fix:
	black $(MONITORED_DIRS)
	ruff check --fix $(MONITORED_DIRS)
	sqlfluff fix --force $(SQLFLUFF_OPTIONS) $(MONITORED_DIRS)

quality:
	black --check $(MONITORED_DIRS)
	ruff check $(MONITORED_DIRS)
	sqlfluff lint $(SQLFLUFF_OPTIONS) $(MONITORED_DIRS)

test:
	pytest -v -W ignore::DeprecationWarning

clean: dbt_clean
	find . -depth -type d -name "__pycache__" -exec rm -rf '{}' \;
	find . -type d -empty -delete


# DBT
# =============================================================================
.PHONY: dbt_clean dbt_docs dbt_run dbt_weekly dbt_daily

dbt_clean:
	rm -rf $(DBT_TARGET_PATH)
	dbt clean

dbt_docs:
	dbt docs generate
	python3 -m http.server --directory $(DBT_TARGET_PATH) --bind 127.0.0.1 $(DBT_DOCS_PORT)

dbt_run:
	dbt run --exclude legacy.oneshot.*+ --exclude marts.oneshot+
	dbt test

dbt_weekly:
	dbt run --select marts.weekly legacy.weekly ephemeral indexed staging
	dbt test

dbt_daily:
	dbt run --select marts.daily legacy.daily ephemeral staging
	dbt test


# Database
# =============================================================================
.PHONY: pg_dump pg_restore load_dump

DUMP_DIR ?= .latest.dump
RESTORE_DIR ?= .latest.restore

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


# Airflow
# =============================================================================
.PHONY: airflow_initdb

airflow_initdb:
	createdb airflow || true
	airflow db reset -y && \
		airflow db upgrade && \
		airflow variables import dag-variables.json && \
		airflow users create --role Admin --email admin@example.com --username admin --password password -f "" -l ""
