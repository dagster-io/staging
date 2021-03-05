import pytest
from dagster import Field, InputContext, OutputContext, fs_io_manager, resource
from dagster.core.errors import DagsterResourceFunctionError
from dagster.core.execution.build_resources import build_resources, initialize_console_manager
from dagster.core.instance import DagsterInstance
from dagster.core.test_utils import instance_for_test


def test_basic_resource():
    @resource
    def basic_resource(_):
        return "foo"

    with DagsterInstance.ephemeral() as dagster_instance:
        with build_resources(
            resource_defs={"basic_resource": basic_resource},
            instance=dagster_instance,
        ) as resources:
            assert resources.resource_instance_dict["basic_resource"] == "foo"


def test_resource_with_config():
    @resource(
        config_schema={"plant": str, "animal": Field(str, is_required=False, default_value="dog")}
    )
    def basic_resource(init_context):
        plant = init_context.resource_config["plant"]
        animal = init_context.resource_config["animal"]
        return f"plant: {plant}, animal: {animal}"

    with DagsterInstance.ephemeral() as dagster_instance:
        with build_resources(
            resource_defs={"basic_resource": basic_resource},
            instance=dagster_instance,
            run_config={"basic_resource": {"config": {"plant": "maple tree"}}},
        ) as resources:
            assert (
                resources.resource_instance_dict["basic_resource"]
                == "plant: maple tree, animal: dog"
            )


def test_resource_with_dependencies():
    @resource(config_schema={"animal": str})
    def no_deps(init_context):
        return init_context.resource_config["animal"]

    @resource(required_resource_keys={"no_deps"})
    def has_deps(init_context):
        return f"{init_context.resources.no_deps} is an animal."

    with DagsterInstance.ephemeral() as dagster_instance:
        with build_resources(
            resource_defs={"no_deps": no_deps, "has_deps": has_deps},
            instance=dagster_instance,
            run_config={"no_deps": {"config": {"animal": "dog"}}},
        ) as resources:
            assert resources.resource_instance_dict["no_deps"] == "dog"
            assert resources.resource_instance_dict["has_deps"] == "dog is an animal."


def test_error_in_resource_initialization():
    @resource
    def i_will_fail(_):
        raise Exception("Failed.")

    with DagsterInstance.ephemeral() as dagster_instance:
        with pytest.raises(
            DagsterResourceFunctionError,
            match="Error executing resource_fn on ResourceDefinition i_will_fail",
        ):
            with build_resources(
                resource_defs={"i_will_fail": i_will_fail}, instance=dagster_instance
            ):
                pass


def fs_io_manager_with_instance_test(instance):
    with build_resources(
        resource_defs={"io_manager": fs_io_manager}, instance=instance
    ) as resources:
        log_manager = initialize_console_manager(None)
        output_context = OutputContext(
            step_key="test_step_key",
            name="result",
            pipeline_name="fake_pipeline",
            run_id="run_id",
            log_manager=log_manager,
        )
        resources.resource_instance_dict["io_manager"].handle_output(
            output_context,
            "foo",
        )
        assert (
            resources.resource_instance_dict["io_manager"].load_input(
                InputContext(
                    name="result",
                    pipeline_name="fake_pipeline",
                    upstream_output=output_context,
                    log_manager=log_manager,
                ),
            )
            == "foo"
        )


def test_build_fs_io_manager():
    with instance_for_test() as instance:
        fs_io_manager_with_instance_test(instance)
