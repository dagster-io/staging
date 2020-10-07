___Dagster___

# dbt Examples

This folder provides examples of how to use `dagster-dbt` to integrate your existing `dbt` project with the Dagster platform.

These examples are here to illustrate how custom solids can be combined with dbt commands in a Dagster pipeline.

### Contents

| Name | Description |
|-|-|
| `dbt_project` | A simple dbt project that loads data from a CSV file and creates views in Postgresql. |
| `setup.sh` | A convenience script that starts up Postgresql with Docker, seeds data from a CSV, and adds the profile for the dbt example project. |
| `my_dbt_pipeline1.py` | A Dagster pipeline definition with a single dbt solid (`dbt_cli_run`). |
| `my_dbt_pipeline2.py` | A Dagster pipeline definition with two dbt solids (`dbt_cli_test` and `dbt_cli_run`). |
| `my_dbt_pipeline3.py` | A Dagster pipeline definition with custom solids along with dbt solids. |

### Running the Examples

1. Make sure you have the necessary Python libraries installed.
Creating a Python virtual env is recommended.
    - `dbt`
    - `dagster`
    - `dagster-dbt`

> __Note__
>
> If you have Docker installed. You can use the convenience setup script `setup.sh` to complete steps #2 and #3.
>
> `$ ./setup.sh`
>
> `$ export DAGSTER_DBT_POSTGRESQL_HOST=127.0.0.1`

2. The dbt project requires a Postgresql database to run.
You can run Postgresql however you like, as long as you set the envvar `DAGSTER_DBT_POSTGRESQL_HOST`.
If you have Docker installed, then you can run Postgresql in a container with [Docker Compose](https://docs.docker.com/compose/install/).

```
$ docker-compose up
$ export $DAGSTER_DBT_POSTGRESQL_HOST=127.0.0.1
```

3. Add the profile for 'dbt_example_project' to your dbt 'profile.yml' file.
Test that this is correctly setup by seeding your Postgresql database with CSV in the dbt example project.

```
$ cat dbt_example_project >> ~/.dbt/profile.yml
$ dbt seed --project-dir dbt_example_project
```

4. Run a local Dagit server with the example pipelines.

```
$ dagit -f my_dbt_pipeline.py
```

### Clean Up

1. If you used Docker Compose to run Postgresql, then you should shut down the resources.

```
$ docker-compose down
```