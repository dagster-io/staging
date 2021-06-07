import os
import tempfile

import mock
import pandas
import pyspark
from dagster import (
    ModeDefinition,
    OutputDefinition,
    ResourceDefinition,
    execute_pipeline,
    pipeline,
    solid,
)
from dagster_pyspark import pyspark_resource
from hacker_news.resources.parquet_io_manager import partitioned_parquet_io_manager
from hacker_news.resources.step_launcher import DataprocStepLauncher
from pyspark.sql import DataFrame as SparkDF


@solid(output_defs=[OutputDefinition(name="out", io_manager_key="pandas_to_spark")])
def emit_pandas_df(_):
    return pandas.DataFrame({"foo": ["bar", "baz"], "quux": [1, 2]})


@solid
def read_pandas_df_to_spark(_, df: SparkDF):
    assert isinstance(df, SparkDF)
    assert df.count() == 2
    assert set(df.columns) == {"foo", "quux"}


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={
                "pyspark": pyspark_resource,
                "pandas_to_spark": partitioned_parquet_io_manager,
                "partition_start": ResourceDefinition.hardcoded_resource("1"),
                "partition_end": ResourceDefinition.hardcoded_resource("2"),
            }
        )
    ]
)
def io_manager_test_pipeline():
    read_pandas_df_to_spark(emit_pandas_df())


def test_io_manager():
    with tempfile.TemporaryDirectory() as temp_dir:
        expected_path = os.path.join(temp_dir, "out-1_2.pq")
        res = execute_pipeline(
            io_manager_test_pipeline,
            run_config={"resources": {"pandas_to_spark": {"config": {"base_path": temp_dir}}}},
        )
        assert res.success
        assert os.path.exists(expected_path)
        intermediate_df = pandas.read_parquet(expected_path)
        assert all(intermediate_df == pandas.DataFrame({"foo": ["bar", "baz"], "quux": [1, 2]}))


EVENTS = [object(), object(), object()]


@mock.patch(
    "hacker_news.resources.step_launcher.DataprocStepLauncher.is_dataproc_step_complete",
    side_effect=[False, False, True],
)
@mock.patch(
    "hacker_news.resources.step_launcher.DataprocStepLauncher.read_events",
    side_effect=[EVENTS[0:1], [], EVENTS[0:3]],
)
@mock.patch(
    "hacker_news.resources.step_launcher.DataprocStepLauncher.handle_output", side_effect=None
)
def test_step_launcher_wait_for_completion(
    _mock_is_dataproc_step_complete, _mock_read_events, _mock_handle_output
):
    context_mock = mock.MagicMock()
    context_mock.resource_config = {"project_id": "foo", "region": "bar"}
    launcher = DataprocStepLauncher(context_mock)
    yielded_events = list(launcher.wait_for_completion(mock.MagicMock(), "", "", 0))
    assert yielded_events == EVENTS
