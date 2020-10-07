#!/bin/bash

set -e  # Stop running this script upon non-zero return code.

# Start Postgresql with Docker Compose (in background).
docker-compose up -d
export DAGSTER_DBT_POSTGRESQL_HOST=127.0.0.1

# Create a '~/.dbt/profiles.yml' file, if it does not already exist.
if [ ! -d "${HOME}/.dbt" ]; then
    echo "Creating ~/.dbt directory."
    mkdir ${HOME}/.dbt
fi
touch ${HOME}/.dbt/profiles.yml

echo "Adding profile 'dbt_example_project' to ~/.dbt/profiles.yml"
cat ./dbt_example_project/profiles.yml >> ${HOME}/.dbt/profiles.yml

# Seed the Postgresql database for the dbt project.
DBT_PROJECT_DIR="$(pwd)/dbt_example_project"
dbt seed --project-dir=${DBT_PROJECT_DIR} --profiles-dir=${DBT_PROJECT_DIR}
