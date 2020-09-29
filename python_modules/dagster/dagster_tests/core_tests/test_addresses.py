import os

import pytest

from dagster import Int, seven
from dagster.core.definitions.address import Address
from dagster.core.errors import DagsterAddressIOError
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.instance import DagsterInstance
from dagster.core.storage.intermediate_storage import build_fs_intermediate_storage


def test_address_path_operation_using_intermediates_file_system():
    with seven.TemporaryDirectory() as tmpdir_path:
        output_address = os.path.join(tmpdir_path, "solid1.output")
        output_value = 5

        instance = DagsterInstance.ephemeral()
        intermediate_storage = build_fs_intermediate_storage(
            instance.intermediates_directory, run_id="some_run_id"
        )

        object_operation_result = intermediate_storage.set_intermediate(
            context=None,
            dagster_type=Int,
            step_output_handle=StepOutputHandle("solid1.compute"),
            value=output_value,
            address=Address(path=output_address),
        )

        assert object_operation_result.key == output_address
        assert object_operation_result.obj == output_value

        assert (
            intermediate_storage.get_intermediate(
                context=None,
                dagster_type=Int,
                step_output_handle=StepOutputHandle("solid1.compute"),
                address=Address(path=output_address),
            ).obj
            == output_value
        )

        with pytest.raises(
            DagsterAddressIOError, match="No such file or directory",
        ):
            intermediate_storage.set_intermediate(
                context=None,
                dagster_type=Int,
                step_output_handle=StepOutputHandle("solid1.compute"),
                value=1,
                address=Address(path="invalid_address"),
            )

        with pytest.raises(
            DagsterAddressIOError, match="No such file or directory",
        ):
            intermediate_storage.get_intermediate(
                context=None,
                dagster_type=Int,
                step_output_handle=StepOutputHandle("solid1.compute"),
                address=Address(path=os.path.join(tmpdir_path, "invalid.output")),
            )
