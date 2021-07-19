import hashlib

import pytest
from dagster import (
    Bool,
    DagsterInvariantViolationError,
    Field,
    Float,
    IOManagerDefinition,
    Int,
    ModeDefinition,
    Output,
    OutputDefinition,
    String,
    composite_solid,
    dagster_type_loader,
    pipeline,
    resource,
    solid,
    usable_as_dagster_type,
)
from dagster.core.definitions import InputDefinition
from dagster.core.execution.api import create_execution_plan
from dagster.core.execution.plan.outputs import StepOutputHandle
from dagster.core.execution.resolve_versions import (
    join_and_hash,
    resolve_config_version,
    resolve_resource_versions,
    resolve_step_output_versions,
    resolve_step_versions,
)
from dagster.core.storage.memoizable_io_manager import MemoizableIOManager
from dagster.core.storage.tags import MEMOIZED_RUN_TAG
from dagster.core.system_config.objects import ResolvedRunConfig
from dagster.core.test_utils import instance_for_test


class VersionedInMemoryIOManager(MemoizableIOManager):
    def __init__(self):
        self.values = {}

    def _get_keys(self, context):
        return (context.step_key, context.name, context.version)

    def handle_output(self, context, obj):
        keys = self._get_keys(context)
        self.values[keys] = obj

    def load_input(self, context):
        keys = self._get_keys(context.upstream_output)
        return self.values[keys]

    def has_output(self, context):
        keys = self._get_keys(context)
        return keys in self.values


def test_join_and_hash():
    assert join_and_hash("foo") == hashlib.sha1(b"foo").hexdigest()

    assert join_and_hash("foo", None, "bar") == None

    assert join_and_hash("foo", "bar") == hashlib.sha1(b"barfoo").hexdigest()

    assert join_and_hash("foo", "bar", "zab") == join_and_hash("zab", "bar", "foo")


def test_resolve_config_version():
    assert resolve_config_version(None) == join_and_hash()

    assert resolve_config_version({}) == join_and_hash()

    assert resolve_config_version({"a": "b", "c": "d"}) == join_and_hash(
        "a" + join_and_hash("b"), "c" + join_and_hash("d")
    )

    assert resolve_config_version({"a": "b", "c": "d"}) == resolve_config_version(
        {"c": "d", "a": "b"}
    )

    assert resolve_config_version({"a": {"b": "c"}, "d": "e"}) == join_and_hash(
        "a" + join_and_hash("b" + join_and_hash("c")), "d" + join_and_hash("e")
    )


@solid(version="42")
def versioned_solid_no_input(_):
    return 4


@solid(version="5")
def versioned_solid_takes_input(_, intput):
    return 2 * intput


def versioned_pipeline_factory(manager=VersionedInMemoryIOManager()):
    @pipeline(
        mode_defs=[
            ModeDefinition(
                name="main",
                resource_defs={"io_manager": IOManagerDefinition.hardcoded_io_manager(manager)},
            )
        ],
        tags={MEMOIZED_RUN_TAG: "true"},
    )
    def versioned_pipeline():
        versioned_solid_takes_input(versioned_solid_no_input())

    return versioned_pipeline


@solid
def solid_takes_input(_, intput):
    return 2 * intput


def partially_versioned_pipeline_factory(manager=VersionedInMemoryIOManager()):
    @pipeline(
        mode_defs=[
            ModeDefinition(
                name="main",
                resource_defs={"io_manager": IOManagerDefinition.hardcoded_io_manager(manager)},
            )
        ],
        tags={MEMOIZED_RUN_TAG: "true"},
    )
    def partially_versioned_pipeline():
        solid_takes_input(versioned_solid_no_input())

    return partially_versioned_pipeline


def versioned_pipeline_expected_step1_version():
    solid1_def_version = versioned_solid_no_input.version
    solid1_config_version = resolve_config_version(None)
    solid1_resources_version = join_and_hash()
    solid1_version = join_and_hash(
        solid1_def_version, solid1_config_version, solid1_resources_version
    )
    return join_and_hash(solid1_version)


def versioned_pipeline_expected_step1_output_version():
    step1_version = versioned_pipeline_expected_step1_version()
    return join_and_hash(step1_version, "result")


def versioned_pipeline_expected_step2_version():
    solid2_def_version = versioned_solid_takes_input.version
    solid2_config_version = resolve_config_version(None)
    solid2_resources_version = join_and_hash()
    solid2_version = join_and_hash(
        solid2_def_version, solid2_config_version, solid2_resources_version
    )
    step1_outputs_hash = versioned_pipeline_expected_step1_output_version()

    step2_version = join_and_hash(step1_outputs_hash, solid2_version)
    return step2_version


def versioned_pipeline_expected_step2_output_version():
    step2_version = versioned_pipeline_expected_step2_version()
    return join_and_hash(step2_version + "result")


def test_step_versions():
    with instance_for_test() as instance:
        versioned_pipeline = versioned_pipeline_factory()
        speculative_execution_plan = create_execution_plan(versioned_pipeline, instance=instance)
        resolved_run_config = ResolvedRunConfig.build(versioned_pipeline)

        versions = resolve_step_versions(
            versioned_pipeline, speculative_execution_plan, resolved_run_config
        )

        assert versions["versioned_solid_no_input"] == versioned_pipeline_expected_step1_version()

        assert (
            versions["versioned_solid_takes_input"] == versioned_pipeline_expected_step2_version()
        )


def test_step_output_versions():
    with instance_for_test() as instance:
        versioned_pipeline = versioned_pipeline_factory()
        speculative_execution_plan = create_execution_plan(
            versioned_pipeline, run_config={}, mode="main", instance=instance
        )
        resolved_run_config = ResolvedRunConfig.build(
            versioned_pipeline, run_config={}, mode="main"
        )

        versions = resolve_step_output_versions(
            versioned_pipeline, speculative_execution_plan, resolved_run_config
        )

        assert (
            versions[StepOutputHandle("versioned_solid_no_input", "result")]
            == versioned_pipeline_expected_step1_output_version()
        )
        assert (
            versions[StepOutputHandle("versioned_solid_takes_input", "result")]
            == versioned_pipeline_expected_step2_output_version()
        )


@solid
def basic_solid(_):
    return 5


@solid
def basic_takes_input_solid(_, intpt):
    return intpt * 4


@pipeline
def no_version_pipeline():
    basic_takes_input_solid(basic_solid())


def test_memoized_plan_no_memoized_results():
    with instance_for_test() as instance:
        versioned_pipeline = versioned_pipeline_factory()
        memoized_plan = create_execution_plan(versioned_pipeline, instance=instance)

        assert set(memoized_plan.step_keys_to_execute) == {
            "versioned_solid_no_input",
            "versioned_solid_takes_input",
        }


def test_memoized_plan_memoized_results():
    with instance_for_test() as instance:
        manager = VersionedInMemoryIOManager()

        versioned_pipeline = versioned_pipeline_factory(manager)
        plan = create_execution_plan(versioned_pipeline, instance=instance)
        resolved_run_config = ResolvedRunConfig.build(versioned_pipeline)

        # Affix a memoized value to the output
        step_output_handle = StepOutputHandle("versioned_solid_no_input", "result")
        step_output_version = plan.get_version_for_step_output_handle(step_output_handle)
        manager.values[
            (step_output_handle.step_key, step_output_handle.output_name, step_output_version)
        ] = 4

        memoized_plan = plan.build_memoized_plan(
            versioned_pipeline, resolved_run_config, instance=None
        )

        assert memoized_plan.step_keys_to_execute == ["versioned_solid_takes_input"]


def test_memoization_no_code_version_for_solid():
    with instance_for_test() as instance:
        partially_versioned_pipeline = partially_versioned_pipeline_factory()

        with pytest.raises(
            DagsterInvariantViolationError,
            match="No version argument provided for solid 'solid_takes_input' when using memoization. "
            "Please provide a version argument to the '@solid' decorator when defining your solid.",
        ):
            create_execution_plan(partially_versioned_pipeline, instance=instance)


def _get_ext_version(config_value):
    return join_and_hash(str(config_value))


@dagster_type_loader(String, loader_version="97", external_version_fn=_get_ext_version)
def InputHydration(_, _hello):
    return "Hello"


@usable_as_dagster_type(loader=InputHydration)
class CustomType(str):
    pass


def test_externally_loaded_inputs():
    for type_to_test, loader_version, type_value in [
        (String, "String", "foo"),
        (Int, "Int", int(42)),
        (Float, "Float", float(5.42)),
        (Bool, "Bool", False),
        (CustomType, "97", "bar"),
    ]:
        run_test_with_builtin_type(type_to_test, loader_version, type_value)


def run_test_with_builtin_type(type_to_test, loader_version, type_value):
    @solid(version="42", input_defs=[InputDefinition("_builtin_type", type_to_test)])
    def versioned_solid_ext_input_builtin_type(_, _builtin_type):
        pass

    @pipeline
    def versioned_pipeline_ext_input_builtin_type():
        versioned_solid_takes_input(versioned_solid_ext_input_builtin_type())

    run_config = {
        "solids": {
            "versioned_solid_ext_input_builtin_type": {"inputs": {"_builtin_type": type_value}}
        }
    }

    with instance_for_test() as instance:
        speculative_execution_plan = create_execution_plan(
            versioned_pipeline_ext_input_builtin_type,
            run_config=run_config,
            instance=instance,
        )

        resolved_run_config = ResolvedRunConfig.build(
            versioned_pipeline_ext_input_builtin_type, run_config=run_config
        )

        versions = resolve_step_versions(
            versioned_pipeline_ext_input_builtin_type,
            speculative_execution_plan,
            resolved_run_config,
        )

        ext_input_version = join_and_hash(str(type_value))
        input_version = join_and_hash(loader_version + ext_input_version)

        solid1_def_version = versioned_solid_ext_input_builtin_type.version
        solid1_config_version = resolve_config_version(None)
        solid1_resources_version = join_and_hash()
        solid1_version = join_and_hash(
            solid1_def_version, solid1_config_version, solid1_resources_version
        )

        step1_version = join_and_hash(input_version, solid1_version)
        assert versions["versioned_solid_ext_input_builtin_type"] == step1_version

        output_version = join_and_hash(step1_version, "result")
        hashed_input2 = output_version

        solid2_def_version = versioned_solid_takes_input.version
        solid2_config_version = resolve_config_version(None)
        solid2_resources_version = join_and_hash()
        solid2_version = join_and_hash(
            solid2_def_version, solid2_config_version, solid2_resources_version
        )

        step2_version = join_and_hash(hashed_input2, solid2_version)
        assert versions["versioned_solid_takes_input"] == step2_version


@solid(
    version="42",
    input_defs=[InputDefinition("default_input", String, default_value="DEFAULTVAL")],
)
def versioned_solid_default_value(_, default_input):
    return default_input * 4


@pipeline
def versioned_pipeline_default_value():
    versioned_solid_default_value()


def test_resolve_step_versions_default_value():
    with instance_for_test() as instance:
        speculative_execution_plan = create_execution_plan(
            versioned_pipeline_default_value, instance=instance
        )
        resolved_run_config = ResolvedRunConfig.build(versioned_pipeline_default_value)

        versions = resolve_step_versions(
            versioned_pipeline_default_value, speculative_execution_plan, resolved_run_config
        )

        input_version = join_and_hash(repr("DEFAULTVAL"))

        solid_def_version = versioned_solid_default_value.version
        solid_config_version = resolve_config_version(None)
        solid_resources_version = join_and_hash()
        solid_version = join_and_hash(
            solid_def_version, solid_config_version, solid_resources_version
        )

        step_version = join_and_hash(input_version, solid_version)
        assert versions["versioned_solid_default_value"] == step_version


@resource(config_schema={"input_str": Field(String)}, version="5")
def basic_resource(context):
    return context.resource_config["input_str"]


@resource(config_schema={"input_str": Field(String)})
def resource_no_version(context):
    return context.resource_config["input_str"]


@resource(version="42")
def resource_no_config(_):
    return "Hello"


@solid(
    required_resource_keys={
        "basic_resource",
        "resource_no_version",
        "resource_no_config",
    },
)
def fake_solid_resources(context):
    return (
        "solidified_"
        + context.resources.basic_resource
        + context.resources.resource_no_version
        + context.resources.resource_no_config
    )


@pipeline(
    mode_defs=[
        ModeDefinition(
            name="fakemode",
            resource_defs={
                "basic_resource": basic_resource,
                "resource_no_version": resource_no_version,
                "resource_no_config": resource_no_config,
            },
        ),
        ModeDefinition(
            name="fakemode2",
            resource_defs={
                "basic_resource": basic_resource,
                "resource_no_version": resource_no_version,
                "resource_no_config": resource_no_config,
            },
        ),
    ]
)
def modes_pipeline():
    fake_solid_resources()


def basic_resource_versions():
    run_config = {
        "resources": {
            "basic_resource": {
                "config": {"input_str": "apple"},
            },
            "resource_no_version": {"config": {"input_str": "banana"}},
        }
    }

    resolved_run_config = ResolvedRunConfig.build(modes_pipeline, run_config, mode="fakemode")

    resource_versions_by_key = resolve_resource_versions(resolved_run_config, modes_pipeline)

    assert resource_versions_by_key["basic_resource"] == join_and_hash(
        resolve_config_version({"input_str": "apple"}), basic_resource.version
    )

    assert resource_versions_by_key["resource_no_version"] == None

    assert resource_versions_by_key["resource_no_config"] == join_and_hash(join_and_hash(), "42")


@solid(required_resource_keys={"basic_resource", "resource_no_config"}, version="39")
def fake_solid_resources_versioned(context):
    return "solidified_" + context.resources.basic_resource + context.resources.resource_no_config


@pipeline(
    mode_defs=[
        ModeDefinition(
            name="fakemode",
            resource_defs={
                "basic_resource": basic_resource,
                "resource_no_config": resource_no_config,
            },
        ),
    ]
)
def versioned_modes_pipeline():
    fake_solid_resources_versioned()


def test_step_versions_with_resources():
    with instance_for_test() as instance:
        run_config = {"resources": {"basic_resource": {"config": {"input_str": "apple"}}}}
        speculative_execution_plan = create_execution_plan(
            versioned_modes_pipeline, run_config=run_config, mode="fakemode", instance=instance
        )
        resolved_run_config = ResolvedRunConfig.build(
            versioned_modes_pipeline, run_config=run_config, mode="fakemode"
        )

        versions = resolve_step_versions(
            versioned_modes_pipeline, speculative_execution_plan, resolved_run_config
        )

        solid_def_version = fake_solid_resources_versioned.version
        solid_config_version = resolve_config_version(None)

        resolved_run_config = ResolvedRunConfig.build(
            versioned_modes_pipeline, run_config, mode="fakemode"
        )

        resource_versions_by_key = resolve_resource_versions(
            resolved_run_config,
            versioned_modes_pipeline,
        )
        solid_resources_version = join_and_hash(
            *[
                resource_versions_by_key[resource_key]
                for resource_key in fake_solid_resources_versioned.required_resource_keys
            ]
        )
        solid_version = join_and_hash(
            solid_def_version, solid_config_version, solid_resources_version
        )

        step_version = join_and_hash(solid_version)

        assert versions["fake_solid_resources_versioned"] == step_version


def test_step_versions_separate_io_manager():
    with instance_for_test() as instance:
        mgr = IOManagerDefinition.hardcoded_io_manager(VersionedInMemoryIOManager())

        @solid(version="39", output_defs=[OutputDefinition(io_manager_key="fake")])
        def solid_requires_io_manager():
            return Output(5)

        @pipeline(
            mode_defs=[
                ModeDefinition(
                    name="fakemode",
                    resource_defs={
                        "fake": mgr,
                    },
                ),
            ]
        )
        def io_mgr_pipeline():
            solid_requires_io_manager()

        speculative_execution_plan = create_execution_plan(
            io_mgr_pipeline, run_config={}, mode="fakemode", instance=instance
        )

        resolved_run_config = ResolvedRunConfig.build(
            io_mgr_pipeline, run_config={}, mode="fakemode"
        )

        versions = resolve_step_versions(
            io_mgr_pipeline, speculative_execution_plan, resolved_run_config
        )

        solid_def_version = fake_solid_resources_versioned.version
        solid_config_version = resolve_config_version(None)
        solid_resources_version = join_and_hash(*[])
        solid_version = join_and_hash(
            solid_def_version, solid_config_version, solid_resources_version
        )
        step_version = join_and_hash(solid_version)
        assert versions["solid_requires_io_manager"] == step_version


def test_unmemoized_inner_solid():
    @solid
    def solid_no_version():
        pass

    @composite_solid
    def wrap():
        return solid_no_version()

    @pipeline(
        mode_defs=[
            ModeDefinition(
                name="fakemode",
                resource_defs={
                    "fake": IOManagerDefinition.hardcoded_io_manager(VersionedInMemoryIOManager()),
                },
            ),
        ],
        tags={MEMOIZED_RUN_TAG: "true"},
    )
    def wrap_pipeline():
        wrap()

    with instance_for_test() as instance:
        with pytest.raises(
            DagsterInvariantViolationError,
            match="No version argument provided for solid 'solid_no_version' when using "
            "memoization. Please provide a version argument to the '@solid' decorator when defining "
            "your solid.",
        ):
            create_execution_plan(wrap_pipeline, instance=instance)


def test_memoized_inner_solid():
    @solid(version="versioned")
    def solid_versioned():
        pass

    @composite_solid
    def wrap():
        return solid_versioned()

    mgr = VersionedInMemoryIOManager()

    @pipeline(
        mode_defs=[
            ModeDefinition(
                name="fakemode",
                resource_defs={
                    "io_manager": IOManagerDefinition.hardcoded_io_manager(mgr),
                },
            ),
        ],
        tags={MEMOIZED_RUN_TAG: "true"},
    )
    def wrap_pipeline():
        wrap()

    with instance_for_test() as instance:
        unmemoized_plan = create_execution_plan(wrap_pipeline, instance=instance)
        step_output_handle = StepOutputHandle("wrap.solid_versioned", "result")
        assert unmemoized_plan.step_keys_to_execute == [step_output_handle.step_key]

        # Affix value to expected version for step output.
        step_output_version = unmemoized_plan.get_version_for_step_output_handle(step_output_handle)
        mgr.values[
            (step_output_handle.step_key, step_output_handle.output_name, step_output_version)
        ] = 4
        memoized_plan = unmemoized_plan.build_memoized_plan(
            wrap_pipeline, ResolvedRunConfig.build(wrap_pipeline), instance=None
        )
        assert len(memoized_plan.step_keys_to_execute) == 0


def test_configured_versions():
    @solid(version="5")
    def solid_to_configure():
        pass

    assert solid_to_configure.configured({}, name="solid_has_been_configured").version == "5"

    @resource(version="5")
    def resource_to_configure(_):
        pass

    assert resource_to_configure.configured({}).version == "5"
