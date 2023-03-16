# Delete target on error.
# https://www.gnu.org/software/make/manual/html_node/Errors.html#Errors
# > This is almost always what you want make to do, but it is not historical
# > practice; so for compatibility, you must explicitly request it
.DELETE_ON_ERROR:

VIRTUAL_ENV ?= .venv
export PATH := $(VIRTUAL_ENV)/bin:$(PATH)

.PHONY: venv clean compile-deps dbt_clean fix_sql

$(VIRTUAL_ENV): requirements.txt
	$(PYTHON_VERSION) -m venv $@
	$@/bin/pip install -r $^

venv: $(VIRTUAL_ENV)


dbt_clean:
	cd airflow && dbt clean

clean: dbt_clean
	find . -type d -name "__pycache__" -depth -exec rm -rf '{}' \;

# Attempt to fix & lint the SQL files. If this does not work,
# try using 'sqlfluff parse' on the target file to see why it
# could not be analyzed and run again.
fix: clean
	sqlfluff fix -f airflow/dbt  --dialect postgres
