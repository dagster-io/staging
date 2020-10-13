#!/bin/bash

set -e  # Stop running this script upon non-zero return code.

pip install -e .

docker-compose up -d

echo "Ensuring ~/.dbt directory and ~/.dbt/profiles.yml file exist."
mkdir -p ${HOME}/.dbt
touch ${HOME}/.dbt/profiles.yml

echo "Adding profile 'dbt_example_project' to ~/.dbt/profiles.yml"
cat ./dbt_example_project/profiles.yml >> ${HOME}/.dbt/profiles.yml

DBT_PROJECT_DIR="$(pwd)/dbt_example_project"
dbt seed --project-dir=${DBT_PROJECT_DIR} --profiles-dir=${DBT_PROJECT_DIR}
