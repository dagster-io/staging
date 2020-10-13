**_Dagster_**

# dbt Examples

This folder provides examples of how to use `dagster-dbt` to integrate your existing `dbt` project
with the Dagster platform.

These examples  illustrate how custom solids can be combined with dbt commands in a Dagster
pipeline.

### Requirements

> If you have Docker and Docker Compose installed, and you aren't already running a Postgres
> database at `localhost:5432`, you can use the convenience script `setup.sh` instead of following
> the steps below.

1. Make sure you have the necessary Python libraries installed. Running inside a Python virtualenv
   is recommended.
   
   ```
   pip install -e .
   ```

2. The example dbt project requires a running Postgres database. If you'd like to run the project
   against your own running database, set the environment variables `DAGSTER_DBT_EXAMPLE_PGHOST`,
   `DAGSTER_DBT_EXAMPLE_PGPORT`, `DAGSTER_DBT_EXAMPLE_PGUSER`, and `DAGSTER_DBT_EXAMPLE_PGPASSWORD`
   as appropriate. By default, the project will attempt to connect to
   `postgresql://dbt-example:dbt-example@localhost:5432/dbt-example`.
   <br /> <br />Or, use Docker Compose to bring up a default database:
   
   ```
   docker-compose up -d
   ```

3. Add the profile for the 'dbt_example_project' to your dbt 'profiles.yml' file.

   ```
   mkdir -p ~/.dbt/profiles.yml
   touch ~/.dbt/profiles.yml
   cat dbt_example_project/profiles.yml >> ~/.dbt/profiles.yml
   ```

   Test that this is correctly setup by running `dbt ls`:

   ```
   dbt ls --project-dir dbt_example_project
   ```


1. Run a local Dagit server with the example pipelines.

```
$ dagit -f my_dbt_pipeline.py
```

### Clean Up

1. If you used Docker Compose to run Postgresql, then you should shut down the resources.

```
$ docker-compose down
```
