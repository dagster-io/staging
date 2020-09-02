from dagster import (
    Array,
    Bool,
    InputDefinition,
    Noneable,
    Nothing,
    OutputDefinition,
    Permissive,
    String,
    StringSource,
    solid,
)
from dagster.config.field import Field

from .utils import DbtCliResult, execute_dbt

DEFAULT_DBT_EXECUTABLE = "dbt"

# https://github.com/fishtown-analytics/dbt/blob/dev/marian-anderson/core/dbt/main.py#L260-L329
CLI_BASE_FLAGS_CONFIG_SCHEMA = {
    "project-dir": Field(
        StringSource,
        is_required=False,
        description="Which directory to look in for the dbt_project.yml file. Default is the current working directory and its parents.",
    ),
    "profiles-dir": Field(
        StringSource,
        is_required=False,
        description="Which directory to look in for the profiles.yml file. Default = $DBT_PROFILES_DIR or $HOME/.dbt",
    ),
    "profile": Field(
        StringSource,
        is_required=False,
        description="Which profile to load. Overrides setting in dbt_project.yml.",
    ),
    "target": Field(
        StringSource, is_required=False, description="Which target to load for the given profile."
    ),
    "vars": Field(
        config=Permissive({}),
        is_required=False,
        description="Supply variables to the project. This argument overrides variables defined in your dbt_project.yml file. This argument should be a dictionary, eg. '{'my_variable': 'my_value'}'",
    ),
    # "log-cache-events": Field(
    #     bool,
    #     is_required=False,
    #     description="If set, log all cache events. This is extremely verbose!",
    # ),
    "bypass-cache": Field(
        bool,
        is_required=False,
        description="If set, bypass the adapter-level cache of database state",
    ),
}

CLI_CONFIG_SCHEMA = {
    **CLI_BASE_FLAGS_CONFIG_SCHEMA,
    "dbt_executable": Field(
        StringSource,
        is_required=False,
        default_value=DEFAULT_DBT_EXECUTABLE,
        description="Path to the dbt binary.",
    ),
    "ignore_handled_error": Field(
        bool,
        default_value=False,
        is_required=False,
        description="When true, will not raise an exception when the dbt CLI returns error code 1.",
    ),
}

CLI_BASE_FLAGS = set(CLI_BASE_FLAGS_CONFIG_SCHEMA.keys())


@solid(
    description="A solid to invoke dbt run via CLI.",
    input_defs=[InputDefinition(name="start_after", dagster_type=Nothing)],
    output_defs=[OutputDefinition(name="result", dagster_type=DbtCliResult)],
    config_schema={
        **CLI_CONFIG_SCHEMA,
        "fail-fast": Field(
            bool, is_required=False, description="Stop execution upon a first failure."
        ),
        "models": Field(
            config=Noneable(Array(String)),
            default_value=None,
            is_required=False,
            description="The dbt models to run.",
        ),
        "exclude": Field(
            config=Noneable(Array(String)),
            default_value=None,
            is_required=False,
            description="The dbt models to exclude.",
        ),
        "full_refresh": Field(
            config=Bool,
            description="Whether or not to perform a --full-refresh.",
            is_required=False,
            # default_value=False,
        ),
        "fail_fast": Field(
            config=Bool,
            description="Whether or not to --fail-fast.",
            is_required=False,
            # default_value=False,
        ),
        "warn_error": Field(
            config=Bool,
            description="Whether or not to --warn-error.",
            is_required=False,
            # default_value=False,
        ),
        "logs": Field(
            config=Bool,
            is_required=False,
            default_value=True,
            description="Whether or not to return logs from the process.",
        ),
        # "task_tags": Permissive(),
    },
    tags={"kind": "dbt"},
)
def dbt_cli_run(context) -> DbtCliResult:
    # filter for CLI_BASE_FLAGS:
    flags_dict = {
        flag: context.solid_config[flag]
        for flag in CLI_BASE_FLAGS
        if context.solid_config.get(flag) is not None
    }

    result: DbtCliResult = execute_dbt(
        context.solid_config["dbt_executable"],
        command="run",
        args_dict=flags_dict,
        log=context.log,
        ignore_handled_error=context.solid_config["ignore_handled_error"],
    )

    return result
