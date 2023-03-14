# Delete target on error.
# https://www.gnu.org/software/make/manual/html_node/Errors.html#Errors
# > This is almost always what you want make to do, but it is not historical
# > practice; so for compatibility, you must explicitly request it
.DELETE_ON_ERROR:

VIRTUAL_ENV ?= .venv
export PATH := $(VIRTUAL_ENV)/bin:$(PATH)

.PHONY: venv clean compile-deps dbt_clean fix_sql

$(VIRTUAL_ENV): requirements-dev.txt
	$(PYTHON_VERSION) -m venv $@
	$@/bin/pip install -r $^

venv: $(VIRTUAL_ENV)


dbt_clean:
	cd airflow_src && dbt clean

clean: dbt_clean
	find . -type d -name "__pycache__" -depth -exec rm -rf '{}' \;
	find . -type d -empty -delete

# Attempt to fix & lint the SQL files. If this does not work,
# try using 'sqlfluff parse' on the target file to see why it
# could not be analyzed and run again.
fix: clean
	black airflow_src
	isort airflow_src
	sqlfluff fix -f airflow_src/dbt --dialect postgres

quality: clean
	black --check airflow_src
	isort --check airflow_src
	flake8 --count --show-source --statistics airflow_src
	sqlfluff lint -f airflow_src/dbt --dialect postgres

start:
	AIRFLOW_BASE_DIR=$(shell pwd)/airflow_src ./airflow_src/entrypoint.sh
