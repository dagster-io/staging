# start_marker_dbt_cli_run
# start_marker_dbt_rpc_run_and_wait
# start_marker_dbt_rpc_run_specific_models
# start_marker_dbt_rpc_run
# start_marker_dbt_cli_run_specific_models
from dagster import ModeDefinition, pipeline
# start_marker_dbt_rpc_resource_example
# start_marker_dbt_rpc_resource
from dagster_dbt import dbt_cli_run, dbt_rpc_resource, dbt_rpc_run, dbt_rpc_run_and_wait

config = {"project-dir": "path/to/dbt/project"}
run_all_models = dbt_cli_run.configured(config, name="run_dbt_project")


@pipeline
def my_dbt_pipeline():
    run_all_models()


# end_marker_dbt_cli_run
dbt_cli_run_solid = run_all_models  # Used for testing
dbt_cli_run_pipeline = my_dbt_pipeline  # Used for testing


config = {"project-dir": "path/to/dbt/project", "models": ["tag:staging"]}
run_staging_models = dbt_cli_run.configured(config, name="run_staging_models")


@pipeline
def my_dbt_pipeline():
    run_staging_models()


# end_marker_dbt_cli_run_specific_models



my_remote_rpc = dbt_rpc_resource.configured({"host": "80.80.80.80", "port": 8080})
# end_marker_dbt_rpc_resource




@pipeline(mode_defs=[ModeDefinition(resource_defs={"dbt_rpc": my_remote_rpc})])
def my_dbt_pipeline():
    dbt_rpc_run()


# end_marker_dbt_rpc_run



run_staging_models = dbt_rpc_run.configured(
    {"models": ["tag:staging"]},
    name="run_staging_models",
)


@pipeline(mode_defs=[ModeDefinition(resource_defs={"dbt_rpc": my_remote_rpc})])
def my_dbt_pipeline():
    run_staging_models()


# end_marker_dbt_rpc_run_specific_models




@pipeline(mode_defs=[ModeDefinition(resource_defs={"dbt_rpc": my_remote_rpc})])
def my_dbt_pipeline():
    dbt_rpc_run_and_wait()


# end_marker_dbt_rpc_run_and_wait


PROFILE_NAME, TARGET_NAME = "", ""
# start_marker_dbt_cli_config_profile_and_target
config = {"profile": PROFILE_NAME, "target": TARGET_NAME}


custom_solid = dbt_cli_run.configured(config, name="custom_solid")
# end_marker_dbt_cli_config_profile_and_target


# start_marker_dbt_cli_config_executable
config = {"dbt_executable": "path/to/dbt/executable"}


custom_solid = dbt_cli_run.configured(config, name="custom_solid")
# end_marker_dbt_cli_config_executable


# start_marker_dbt_cli_config_select_models
config = {"models": ["my_dbt_model+", "path.to.models", "tag:nightly"]}


custom_solid = dbt_cli_run.configured(config, name="custom_solid")
# end_marker_dbt_cli_config_select_models


# start_marker_dbt_cli_config_exclude_models
config = {"exclude": ["my_dbt_model+", "path.to.models", "tag:nightly"]}


custom_solid = dbt_cli_run.configured(config, name="custom_solid")
# end_marker_dbt_cli_config_exclude_models


# start_marker_dbt_cli_config_vars
config = {"vars": {"key": "value"}}


custom_solid = dbt_cli_run.configured(config, name="custom_solid")
# end_marker_dbt_cli_config_vars


# start_marker_dbt_cli_config_disable_assets
config = {"yield_materializations": False}


custom_solid = dbt_cli_run.configured(config, name="custom_solid")
# end_marker_dbt_cli_config_disable_assets


HOST, PORT = "", ""

custom_resource = dbt_rpc_resource.configured({"host": HOST, "post": PORT})
# end_marker_dbt_rpc_resource_example


# start_marker_dbt_rpc_config_select_models
config = {"models": ["my_dbt_model+", "path.to.models", "tag:nightly"]}


custom_solid = dbt_rpc_run.configured(config, name="custom_solid")
# end_marker_dbt_rpc_config_select_models


# start_marker_dbt_rpc_config_exclude_models
config = {"exclude": ["my_dbt_model+", "path.to.models", "tag:nightly"]}


custom_solid = dbt_rpc_run.configured(config, name="custom_solid")
# end_marker_dbt_rpc_config_exclude_models


# start_marker_dbt_rpc_and_wait_config_polling_interval
config = {"interval": 3}  # Poll the dbt RPC server every 3 seconds.


custom_solid = dbt_rpc_run.configured(config, name="custom_solid")
# end_marker_dbt_rpc_and_wait_config_polling_interval


# start_marker_dbt_rpc_config_disable_assets
config = {"yield_materializations": False}


custom_solid = dbt_rpc_run.configured(config, name="custom_solid")
# end_marker_dbt_rpc_config_disable_assets
