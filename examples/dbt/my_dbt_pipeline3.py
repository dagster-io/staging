from dagster_dbt import dbt_cli_run, dbt_cli_test

from dagster import pipeline, solid

PROFILES_DIR = "~/.dbt"
PROJECT_DIR = "./dbt_example_project"


# Solid Definition
@solid(config_schema={"email": str})
def notify(context, run_results):
    recipient = context.solid_config["email"]
    context.log.info(f"Sending dbt run results to {recipient}.")
    email_run_results(recipient, run_results)


# Solid Configuration
run = dbt_cli_run.configured(
    name="run", config_or_config_fn={"project-dir": PROJECT_DIR, "profiles-dir": PROFILES_DIR},
)

test = dbt_cli_test.configured(
    name="test", config_or_config_fn={"project-dir": PROJECT_DIR, "profiles-dir": PROFILES_DIR},
)


notify_alice = notify.configured(
    name="notify_alice", config_or_config_fn={"email": "alice@dagster.io"}
)

notify_bob = notify.configured(name="notify_bob", config_or_config_fn={"email": "bob@dagster.io"})


@pipeline
def my_pipeline():
    run_results = run(start_after=test())
    notify_alice(run_results)
    notify_bob(run_results)


def email_run_results(recipient, run_results):  # pylint: disable=unused-argument
    print(f"Sending run results to {recipient}")  # pylint: disable=print-statement
