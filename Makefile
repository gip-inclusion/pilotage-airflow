# Delete target on error.
# https://www.gnu.org/software/make/manual/html_node/Errors.html#Errors
# > This is almost always what you want make to do, but it is not historical
# > practice; so for compatibility, you must explicitly request it
.DELETE_ON_ERROR:

PYTHON_VERSION := python3.11
ifeq ($(shell uname -s),Linux)
	REQUIREMENTS_PATH := requirements.txt
else
	REQUIREMENTS_PATH := requirements.in
endif

VIRTUAL_ENV ?= .venv
export PATH := $(VIRTUAL_ENV)/bin:$(PATH)

.PHONY: venv clean compile-deps dbt_clean fix_sql

$(VIRTUAL_ENV): $(REQUIREMENTS_PATH)
	$(PYTHON_VERSION) -m venv $@
	$@/bin/pip install -r $^
ifeq ($(shell uname -s),Linux)
	$@/bin/pip-sync $^
endif
	touch $@

venv: $(VIRTUAL_ENV)

PIP_COMPILE_FLAGS := --upgrade --allow-unsafe --generate-hashes
compile-deps: $(VIRTUAL_ENV)
	pip-compile $(PIP_COMPILE_FLAGS) -o requirements.txt requirements.in

dbt_clean:
	cd airflow && dbt clean

clean: dbt_clean
	find . -type d -name "__pycache__" -depth -exec rm -rf '{}' \;

# Attempt to fix & lint the SQL files. If this does not work,
# try using 'sqlfluff parse' on the target file to see why it
# could not be analyzed and run again.
fix: clean
	sqlfluff fix -f airflow/dbt  --dialect postgres
