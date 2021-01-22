import json
import os
import pickle
import tempfile
from contextlib import contextmanager

import nbformat
import pytest
from dagster import (
    ModeDefinition,
    check,
    execute_pipeline,
    fs_io_manager,
    local_file_manager,
    pipeline,
)
from dagster.core.definitions.events import PathMetadataEntryData
from dagster.core.definitions.reconstructable import ReconstructablePipeline
from dagster.core.test_utils import instance_for_test
from dagster.utils import file_relative_path, mkdir_p, safe_tempfile_path
from dagstermill import DagstermillError, dagstermill_solid, define_dagstermill_solid
from jupyter_client.kernelspec import NoSuchKernel
from nbconvert.preprocessors import ExecutePreprocessor
from papermill import PapermillExecutionError

try:
    import dagster_pandas as _

    DAGSTER_PANDAS_PRESENT = True
except ImportError:
    DAGSTER_PANDAS_PRESENT = False

try:
    import sklearn as _

    SKLEARN_PRESENT = True
except ImportError:
    SKLEARN_PRESENT = False

try:
    import matplotlib as _

    MATPLOTLIB_PRESENT = True
except ImportError:
    MATPLOTLIB_PRESENT = False


def get_path(materialization_event):
    metadata_entries = materialization_event.event_specific_data.materialization.metadata_entries
    for metadata_entry in metadata_entries:
        if isinstance(metadata_entry.entry_data, PathMetadataEntryData):
            return metadata_entry.entry_data.path


def cleanup_result_notebook(result):
    if not result:
        return
    materialization_events = [
        x for x in result.step_event_list if x.event_type_value == "STEP_MATERIALIZATION"
    ]
    for materialization_event in materialization_events:
        result_path = get_path(materialization_event)
        if os.path.exists(result_path):
            os.unlink(result_path)


@contextmanager
def exec_for_test(
    fn_name, run_config=None, raise_on_error=True, legacy=False, mode=None, **kwargs,
):
    run_config = check.opt_dict_param(run_config, "run_config")
    result = None
    if legacy:
        fn_name = f"{fn_name}_legacy"
    recon_pipeline = ReconstructablePipeline.for_module("dagstermill.examples.repository", fn_name)

    with tempfile.TemporaryDirectory() as temp_dir:
        file_manager_base_dir = os.path.join(temp_dir, "file_manager")
        mkdir_p(file_manager_base_dir)
        if "resources" not in run_config:
            run_config["resources"] = (
                {"io_manager": {"config": {"base_dir": temp_dir}},}
                if not legacy
                else {"file_manager": {"config": {"base_dir": file_manager_base_dir}}}
            )
        else:
            if legacy and "file_manager" not in run_config["resources"]:
                run_config["resources"]["file_manager"] = {
                    "config": {"base_dir": file_manager_base_dir}
                }
            if not legacy and "io_manager" not in run_config["resources"]:
                run_config["resources"]["io_manager"] = {"config": {"base_dir": temp_dir}}

        with instance_for_test() as instance:
            try:
                result = execute_pipeline(
                    recon_pipeline,
                    run_config,
                    mode=mode or ("test" if not legacy else "legacy_test"),
                    instance=instance,
                    raise_on_error=raise_on_error,
                    **kwargs,
                )
                yield result
            finally:
                if result:
                    cleanup_result_notebook(result)


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_hello_world(legacy):
    with exec_for_test("hello_world_pipeline", legacy=legacy) as result:
        assert result.success


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_hello_world_with_config(legacy):
    with exec_for_test("hello_world_config_pipeline", legacy=legacy) as result:
        assert result.success


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_hello_world_with_output_notebook(legacy):
    solid_name = "hello_world" + ("_legacy" if legacy else "")
    with exec_for_test("hello_world_with_output_notebook_pipeline", legacy=legacy) as result:
        assert result.success
        assert result.result_for_solid(solid_name).success
        assert "notebook" in result.result_for_solid(solid_name).output_values
        if legacy:
            materializations = [
                x for x in result.event_list if x.event_type_value == "STEP_MATERIALIZATION"
            ]
            assert len(materializations) == 1

            assert os.path.exists(
                result.result_for_solid(solid_name).output_values["notebook"].path_desc
            )
            assert (
                materializations[0]
                .event_specific_data.materialization.metadata_entries[0]
                .entry_data.path
                == result.result_for_solid(solid_name).output_values["notebook"].path_desc
            )

        assert result.result_for_solid("load_notebook" + ("_legacy" if legacy else "")).success
        assert (
            result.result_for_solid("load_notebook" + ("_legacy" if legacy else "")).output_value()
            is True
        )


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_hello_world_with_config_escape(legacy):
    solid_name = "hello_world_config" + ("_legacy" if legacy else "")
    with exec_for_test(
        "hello_world_config_pipeline",
        run_config={"solids": {solid_name: {"config": {"greeting": "'"}}}},
        legacy=legacy,
    ) as result:
        assert result.success

    with exec_for_test(
        "hello_world_config_pipeline",
        run_config={"solids": {solid_name: {"config": {"greeting": '"'}}}},
        legacy=legacy,
    ) as result:
        assert result.success

    with exec_for_test(
        "hello_world_config_pipeline",
        run_config={"solids": {solid_name: {"config": {"greeting": "\\"}}}},
        legacy=legacy,
    ) as result:
        assert result.success

    with exec_for_test(
        "hello_world_config_pipeline",
        run_config={"solids": {solid_name: {"config": {"greeting": "}"}}}},
        legacy=legacy,
    ) as result:
        assert result.success

    with exec_for_test(
        "hello_world_config_pipeline",
        run_config={"solids": {solid_name: {"config": {"greeting": "\n"}}}},
        legacy=legacy,
    ) as result:
        assert result.success


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_reexecute_result_notebook(legacy):
    with exec_for_test(
        "hello_world_pipeline",
        run_config={"loggers": {"console": {"config": {"log_level": "ERROR"}}}},
        legacy=legacy,
    ) as result:
        assert result.success

        materialization_events = [
            x for x in result.step_event_list if x.event_type_value == "STEP_MATERIALIZATION"
        ]
        for materialization_event in materialization_events:
            result_path = get_path(materialization_event)

            if result_path.endswith(".ipynb"):
                with open(result_path) as fd:
                    nb = nbformat.read(fd, as_version=4)
                ep = ExecutePreprocessor()
                ep.preprocess(nb, {})
                with open(result_path) as fd:
                    assert nbformat.read(fd, as_version=4) == nb


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_hello_world_with_output(legacy):
    with exec_for_test("hello_world_output_pipeline", legacy=legacy) as result:
        assert result.success
        assert (
            result.result_for_solid(
                "hello_world_output" + ("_legacy" if legacy else "")
            ).output_value()
            == "hello, world"
        )


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_hello_world_explicit_yield(legacy):
    with exec_for_test("hello_world_explicit_yield_pipeline", legacy=legacy) as result:
        materializations = [
            x for x in result.event_list if x.event_type_value == "STEP_MATERIALIZATION"
        ]
        assert len(materializations) == (2 if legacy else 1)
        assert get_path(materializations[-1]) == "/path/to/file"


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_add_pipeline(legacy):
    with exec_for_test(
        "add_pipeline", {"loggers": {"console": {"config": {"log_level": "ERROR"}}}}, legacy=legacy,
    ) as result:
        assert result.success
        assert (
            result.result_for_solid(
                "add_two_numbers" + ("_legacy" if legacy else "")
            ).output_value()
            == 3
        )


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_notebook_dag(legacy):
    with exec_for_test(
        "notebook_dag_pipeline",
        {"solids": {"load_a": {"config": 1}, "load_b": {"config": 2}}},
        legacy=legacy,
    ) as result:
        assert result.success
        assert (
            result.result_for_solid(
                "add_two_numbers" + ("_legacy" if legacy else "")
            ).output_value()
            == 3
        )
        assert (
            result.result_for_solid(
                "mult_two_numbers" + ("_legacy" if legacy else "")
            ).output_value()
            == 6
        )


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_error_notebook(legacy):
    with pytest.raises(PapermillExecutionError) as exc:
        with exec_for_test("error_pipeline", legacy=legacy) as result:
            pass

    assert "Someone set up us the bomb" in str(exc.value)

    with exec_for_test("error_pipeline", raise_on_error=False, legacy=legacy) as result:
        assert not result.success
        if legacy:
            assert result.step_event_list[1].event_type.value == "STEP_MATERIALIZATION"
            assert result.step_event_list[2].event_type.value == "STEP_FAILURE"


@pytest.mark.nettest
@pytest.mark.notebook_test
@pytest.mark.skipif(
    not (DAGSTER_PANDAS_PRESENT and SKLEARN_PRESENT and MATPLOTLIB_PRESENT),
    reason="tutorial_pipeline reqs not present: dagster_pandas, sklearn, matplotlib",
)
@pytest.mark.parametrize("legacy", [True, False])
def test_tutorial_pipeline(legacy):
    with exec_for_test(
        "tutorial_pipeline",
        {"loggers": {"console": {"config": {"log_level": "DEBUG"}}}},
        legacy=legacy,
    ) as result:
        assert result.success


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_hello_world_reexecution(legacy):
    with exec_for_test("hello_world_pipeline", legacy=legacy) as result:
        assert result.success
        with safe_tempfile_path() as output_notebook_path:

            if legacy:
                output_notebook_path = get_path(
                    [
                        x
                        for x in result.step_event_list
                        if x.event_type_value == "STEP_MATERIALIZATION"
                    ][0]
                )
            else:
                with open(output_notebook_path, "wb") as fd:
                    fd.write(result.result_for_solid("hello_world").output_value("notebook"))

            with tempfile.NamedTemporaryFile("w+", suffix=".py") as reexecution_pipeline_file:
                if legacy:
                    reexecution_pipeline_file.write(
                        (
                            "from dagster import (\n"
                            "    ModeDefinition,\n"
                            "    fs_io_manager,\n"
                            "    local_file_manager,\n"
                            "    pipeline,\n"
                            ")\n"
                            "from dagstermill import define_dagstermill_solid\n\n\n"
                            "reexecution_solid = define_dagstermill_solid(\n"
                            f"    'hello_world_reexecution', '{output_notebook_path}'\n"
                            ")\n\n"
                            "@pipeline(\n"
                            "    mode_defs=[\n"
                            "        ModeDefinition(\n"
                            "            resource_defs={\n"
                            "               'file_manager': local_file_manager\n"
                            "            },\n"
                            "       ),\n"
                            "    ],\n"
                            ")\n"
                            "def reexecution_pipeline():\n"
                            "    reexecution_solid()\n"
                        )
                    )
                else:
                    reexecution_pipeline_file.write(
                        (
                            "from dagster import (\n"
                            "    ModeDefinition,\n"
                            "    fs_io_manager,\n"
                            "    local_file_manager,\n"
                            "    pipeline,\n"
                            ")\n"
                            "from dagstermill import dagstermill_solid\n\n\n"
                            "reexecution_solid = dagstermill_solid(\n"
                            f"    'hello_world_reexecution', '{output_notebook_path}'\n"
                            ")\n\n"
                            "@pipeline(\n"
                            "    mode_defs=[\n"
                            "        ModeDefinition(\n"
                            "            resource_defs={\n"
                            "               'io_manager': fs_io_manager,\n"
                            "               'file_manager': local_file_manager\n"
                            "            },\n"
                            "       ),\n"
                            "    ],\n"
                            ")\n"
                            "def reexecution_pipeline():\n"
                            "    reexecution_solid()\n"
                        )
                    )
                reexecution_pipeline_file.flush()

                result = None
                reexecution_pipeline = ReconstructablePipeline.for_file(
                    reexecution_pipeline_file.name, "reexecution_pipeline"
                )

                reexecution_result = None
                with instance_for_test() as instance:
                    try:
                        with tempfile.TemporaryDirectory() as temp_dir:
                            reexecution_result = execute_pipeline(
                                reexecution_pipeline,
                                instance=instance,
                                run_config=(
                                    {
                                        "resources": {
                                            "io_manager": {"config": {"base_dir": temp_dir}}
                                        }
                                    }
                                    if not legacy
                                    else {}
                                ),
                            )
                        assert reexecution_result.success
                    finally:
                        if reexecution_result:
                            cleanup_result_notebook(reexecution_result)


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_resources_notebook(legacy):
    with safe_tempfile_path() as path:
        with exec_for_test(
            "resource_pipeline",
            run_config={"resources": {"list": {"config": path}}},
            mode="prod",
            legacy=legacy,
        ) as result:
            assert result.success

            # Expect something like:
            # ['e8d636: Opened', 'e8d636: Hello, solid!', '9d438e: Opened',
            #  '9d438e: Hello, notebook!', '9d438e: Closed', 'e8d636: Closed']
            with open(path, "rb") as fd:
                messages = pickle.load(fd)

            messages = [message.split(": ") for message in messages]

            resource_ids = [x[0] for x in messages]
            assert len(set(resource_ids)) == 2
            assert resource_ids[0] == resource_ids[1] == resource_ids[5]
            assert resource_ids[2] == resource_ids[3] == resource_ids[4]

            msgs = [x[1] for x in messages]
            assert msgs[0] == msgs[2] == "Opened"
            assert msgs[4] == msgs[5] == "Closed"
            assert msgs[1] == "Hello, solid!"
            assert msgs[3] == "Hello, notebook!"


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_resources_notebook_with_exception(legacy):
    result = None
    with safe_tempfile_path() as path:
        with exec_for_test(
            "resource_with_exception_pipeline",
            run_config={"resources": {"list": {"config": path}}},
            raise_on_error=False,
            mode="default",
            legacy=legacy,
        ) as result:
            assert not result.success
            assert result.step_event_list[-1].event_type.value == "STEP_FAILURE"
            assert (
                "raise Exception()" in result.step_event_list[-1].event_specific_data.error.message
            )

            # Expect something like:
            # ['e8d636: Opened', 'e8d636: Hello, solid!', '9d438e: Opened',
            #  '9d438e: Hello, notebook!', '9d438e: Closed', 'e8d636: Closed']
            with open(path, "rb") as fd:
                messages = pickle.load(fd)

            messages = [message.split(": ") for message in messages]

            resource_ids = [x[0] for x in messages]
            assert len(set(resource_ids)) == 2
            assert resource_ids[0] == resource_ids[1] == resource_ids[5]
            assert resource_ids[2] == resource_ids[3] == resource_ids[4]

            msgs = [x[1] for x in messages]
            assert msgs[0] == msgs[2] == "Opened"
            assert msgs[4] == msgs[5] == "Closed"
            assert msgs[1] == "Hello, solid!"
            assert msgs[3] == "Hello, notebook!"


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_bad_kernel_pipeline(legacy):
    with pytest.raises(NoSuchKernel):
        with exec_for_test("bad_kernel_pipeline", legacy=legacy):
            pass


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_hello_logging(legacy):
    with exec_for_test("hello_logging_pipeline", legacy=legacy) as result:
        assert result.success


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_reimport(legacy):
    with exec_for_test("reimport_pipeline", legacy=legacy) as result:
        assert result.success
        assert (
            result.result_for_solid("reimport" + ("_legacy" if legacy else "")).output_value() == 6
        )


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_yield_3_pipeline(legacy):
    with exec_for_test("yield_3_pipeline", legacy=legacy) as result:
        assert result.success
        assert (
            result.result_for_solid("yield_3" + ("_legacy" if legacy else "")).output_value() == 3
        )


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_yield_obj_pipeline(legacy):
    with exec_for_test("yield_obj_pipeline", legacy=legacy) as result:
        assert result.success
        assert (
            result.result_for_solid("yield_obj" + ("_legacy" if legacy else "")).output_value().x
            == 3
        )


def test_non_reconstructable_pipeline_legacy():
    foo_solid = define_dagstermill_solid("foo", file_relative_path(__file__, "notebooks/foo.ipynb"))

    @pipeline(
        mode_defs=[
            ModeDefinition(
                resource_defs={"io_manager": fs_io_manager, "file_manager": local_file_manager}
            )
        ]
    )
    def non_reconstructable():
        foo_solid()

    with pytest.raises(DagstermillError, match="pipeline that is not reconstructable."):
        execute_pipeline(non_reconstructable)


def test_non_reconstructable_pipeline():
    foo_solid = dagstermill_solid("foo", file_relative_path(__file__, "notebooks/foo.ipynb"))

    @pipeline(
        mode_defs=[
            ModeDefinition(
                resource_defs={"io_manager": fs_io_manager, "file_manager": local_file_manager}
            )
        ]
    )
    def non_reconstructable():
        foo_solid()

    with pytest.raises(DagstermillError, match="pipeline that is not reconstructable."):
        execute_pipeline(non_reconstructable)


@pytest.mark.notebook_test
@pytest.mark.parametrize("legacy", [True, False])
def test_sum_nums_pipeline(legacy):
    with exec_for_test("sum_nums_pipeline", legacy=legacy) as result:
        assert result.success
        assert (
            result.result_for_solid("sum_nums" + ("_legacy" if legacy else "")).output_value() == 18
        )


@pytest.mark.notebook_test
def test_nothing_pipeline():
    with exec_for_test("nothing_pipeline") as result:
        assert result.success
        assert result.result_for_solid("nothing").output_value() == 6


@pytest.mark.notebook_test
def test_custom_output_notebook_io_manager_pipeline():
    with tempfile.TemporaryDirectory() as temp_dir:
        file_target = os.path.join(temp_dir, "out.ipynb")
        with exec_for_test(
            "custom_output_notebook_io_manager_pipeline",
            {"resources": {"file_io_manager": {"config": {"file_target": file_target}},}},
        ) as result:
            assert result.success
            with open(file_target, "r") as fd:
                nb = json.load(fd)
                assert "cells" in nb


@pytest.mark.notebook_test
def test_custom_output_manager_pipeline():
    with exec_for_test("custom_output_manager_pipeline") as result:
        assert result.success
        assert result.result_for_solid("add_two_numbers").output_value() == 7
