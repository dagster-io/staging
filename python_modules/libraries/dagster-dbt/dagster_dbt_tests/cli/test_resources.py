import json

from dagster import ModeDefinition, execute_solid, solid
from dagster_dbt import dbt_cli_resource


def get_dbt_resource(project_dir, profiles_dir, extra_flags=None):
    extra_flags = extra_flags or {}
    return dbt_cli_resource.configured(
        {
            "default_flags": {
                "project-dir": project_dir,
                "profiles-dir": profiles_dir,
                **extra_flags,
            }
        }
    )


def get_mode_def(project_dir, profiles_dir):
    return ModeDefinition(resource_defs={"dbt": get_dbt_resource(project_dir, profiles_dir)})


def test_seed(conn_string, test_project_dir, dbt_config_dir):  # pylint: disable=unused-argument
    @solid(required_resource_keys={"dbt"})
    def my_dbt_solid(context):
        return context.resources.dbt.seed()

    dbt_result = execute_solid(
        my_dbt_solid, get_mode_def(test_project_dir, dbt_config_dir)
    ).output_value()
    assert len(dbt_result.result.results) == 1


def test_test(
    dbt_seed, conn_string, test_project_dir, dbt_config_dir
):  # pylint: disable=unused-argument
    @solid(required_resource_keys={"dbt"})
    def my_dbt_solid(context):
        context.resources.dbt.run()
        return context.resources.dbt.test()

    dbt_result = execute_solid(
        my_dbt_solid, get_mode_def(test_project_dir, dbt_config_dir)
    ).output_value()
    assert len(dbt_result.result.results) == 15


def test_basic_run(
    dbt_seed, conn_string, test_project_dir, dbt_config_dir
):  # pylint: disable=unused-argument
    @solid(required_resource_keys={"dbt"})
    def my_dbt_solid(context):
        return context.resources.dbt.run()

    dbt_result = execute_solid(
        my_dbt_solid, get_mode_def(test_project_dir, dbt_config_dir)
    ).output_value()
    assert len(dbt_result.result.results) == 4


def test_extra_flags_run(
    dbt_seed, conn_string, test_project_dir, dbt_config_dir
):  # pylint: disable=unused-argument

    my_vars = {"foo": 1, "bar": "baz"}

    @solid(required_resource_keys={"dbt"})
    def my_dbt_solid(context):
        return context.resources.dbt.run(extra_flags={"vars": my_vars})

    dbt_result = execute_solid(
        my_dbt_solid, get_mode_def(test_project_dir, dbt_config_dir)
    ).output_value()
    assert len(dbt_result.result.results) == 4
    assert json.loads(dbt_result.result.raw_run_results["args"]["vars"]) == my_vars


def test_merged_extra_flags_run(
    dbt_seed, conn_string, test_project_dir, dbt_config_dir
):  # pylint: disable=unused-argument

    configured_vars = {"hello": "world"}
    my_vars = {"foo": 1, "bar": "baz"}

    @solid(required_resource_keys={"dbt"})
    def my_dbt_solid(context):
        return context.resources.dbt.run(extra_flags={"vars": my_vars})

    dbt_result = execute_solid(
        my_dbt_solid,
        ModeDefinition(
            resource_defs={
                "dbt": get_dbt_resource(
                    test_project_dir,
                    dbt_config_dir,
                    extra_flags={"vars": configured_vars},
                )
            }
        ),
    ).output_value()
    assert len(dbt_result.result.results) == 4
    for key in [*configured_vars.keys(), *my_vars.keys()]:
        assert key in dbt_result.command
    assert json.loads(dbt_result.result.raw_run_results["args"]["vars"]) == {
        **configured_vars,
        **my_vars,
    }
