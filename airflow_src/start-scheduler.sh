#!/bin/bash

set -e
set -x

source _source-vars.sh

airflow scheduler
