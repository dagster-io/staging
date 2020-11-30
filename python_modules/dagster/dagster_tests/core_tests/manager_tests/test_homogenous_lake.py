"""
This test encapsulates the case where you have a graph which is homogenous set of
assets where the inputs and outputs are clean and match types. The actual generated
artifacts are in fixed location and are overwritten with every computation. If
there is versioning, etc, it is done in the storage layer. In cases such as these,
it is highly convenient to only annotate the output in the default case and allow
that to determine the behavior of both the output in question and the downstream input

This scenario is realistic for a greenfield case controlled by a single team.
"""
import os
from tempfile import TemporaryDirectory

import pytest
from dagster import InputDefinition as RealInputDefinition
from dagster import OutputDefinition as RealOutputDefinition
from dagster import PythonObjectDagsterType, check, execute_pipeline, pipeline, solid
from dagster.utils import ensure_dir

# Just going to pass around strings and pretend it is a dataframe
HomogenousLakeDataFrame = PythonObjectDagsterType(name="HomogenousLakeDataFrame", python_type=str)


def InputDefinition(
    *args, manager_key=None, metadata=None, **kwargs
):  # pylint: disable=unused-argument
    return RealInputDefinition(*args, **kwargs)


def OutputDefinition(
    *args, manager_key=None, metadata=None, **kwargs
):  # pylint: disable=unused-argument
    return RealOutputDefinition(*args, **kwargs)


# locally we store strings in non-binary format for ease of debugging
class LocalHomogenousDataLake:
    def __init__(self, base_dir):
        self.base_dir = base_dir

    def _path_for_coordinate(self, coordinate):
        return os.path.join(self.base_dir, coordinate)

    def load(self, coordinate):
        check.str_param(coordinate, "coordinate")
        with open(self._path_for_coordinate(coordinate), "r") as read_obj:
            return read_obj.read()

    def store(self, coordinate, obj):
        check.str_param(coordinate, "coordinate")
        check.str_param(obj, "obj")

        with open(self._path_for_coordinate(coordinate), "w") as write_obj:
            write_obj.write(obj)


# In prod we store strings in a "cloud filesystem" in a binary format


# This mimics an s3 resource or similar
class CloudBucket:
    @staticmethod
    def create_with_dir(base_dir, bucket):
        ensure_dir(os.path.join(base_dir, bucket))
        return CloudBucket(base_dir, bucket)

    def __init__(self, base_dir, bucket):
        self.base_dir = base_dir
        self.bucket = bucket

    def _abs_path(self, path):
        return os.path.join(self.base_dir, self.bucket, path)

    def put_bytes(self, path, binary_data):
        check.str_param(path, "path")
        check.param_invariant(isinstance(binary_data, bytes), "binary_data")
        with open(self._abs_path(path), "wb") as write_obj:
            write_obj.write(binary_data)

    def get_bytes(self, path):
        check.str_param(path, "path")
        with open(self._abs_path(path), "rb") as read_obj:
            return read_obj.read()


class ProductionHomogenousDataLake:
    def __init__(self, cloud_bucket):
        self.cloud_bucket = cloud_bucket

    def load(self, coordinate):
        check.str_param(coordinate, "coordinate")
        return self.cloud_bucket.get_bytes(coordinate).decode()

    def store(self, coordinate, obj):
        check.str_param(coordinate, "coordinate")
        check.str_param(obj, "obj")
        self.cloud_bucket.put_bytes(coordinate, obj.encode())


@solid
def return_foo(_):
    return "foo"


@solid(
    input_defs=[
        InputDefinition(
            "df",
            HomogenousLakeDataFrame,
            manager_key="data_lake_manager",
            metadata={"data_lake_coordinate": "source_table"},
        )
    ],
    output_defs=[
        OutputDefinition(
            HomogenousLakeDataFrame,
            manager_key="data_lake_manager",
            metadata={"data_lake_coordinate": "first_table"},
        )
    ],
)
def standalone_input_solid(_, df: str) -> str:
    return df + "-first"


@solid(
    # note that this input should be able to get metadata from the logical upstream output
    input_defs=[InputDefinition("df", HomogenousLakeDataFrame)],
    output_defs=[
        OutputDefinition(
            HomogenousLakeDataFrame,
            manager_key="data_lake_manager",
            metadata={"data_lake_coordinate": "middle_table"},
        )
    ],
)
def in_graph_solid(_, df: str) -> str:
    return df + "-middle"


@solid(
    # note that this input should be able to get metadata from the logical upstream output
    input_defs=[InputDefinition("df", HomogenousLakeDataFrame)],
    output_defs=[
        OutputDefinition(
            HomogenousLakeDataFrame,
            manager_key="data_lake_manager",
            metadata={"data_lake_coordinate": "last_table"},
        )
    ],
)
def hanging_output_solid(_, df: str) -> str:
    return df + "-last"


@pipeline
def homogenous_data_lake_with_input_stub():
    hanging_output_solid(in_graph_solid(standalone_input_solid(return_foo())))


def test_lake_with_input_stub():
    result = execute_pipeline(homogenous_data_lake_with_input_stub)
    assert result.success
    assert result.output_for_solid("hanging_output_solid") == "foo-first-middle-last"


@pytest.mark.xfail(reason="Expected to fail because of unsatisfied input")
def test_data_lake():
    @pipeline
    def homogenous_data_lake():
        hanging_output_solid(in_graph_solid(standalone_input_solid()))

    # TODO: write manager that has some scheme of storing files on the filesystem
    # and then make it work here
    assert homogenous_data_lake


# just testing implementation before plugging into "manager" or "asset store"
def test_local_homogenous_data_lake():
    with TemporaryDirectory() as tmp_dir:
        data_lake = LocalHomogenousDataLake(tmp_dir)

        data_lake.store("a_table", "foo")

        # ensure file exists
        os.path.exists(os.path.join(tmp_dir, "foo"))

        assert data_lake.load("a_table") == "foo"


# just testing implementation before plugging into "manager" or "asset store"
def test_prod_homogenous_data_lake():
    with TemporaryDirectory() as tmp_dir:
        bucket = CloudBucket.create_with_dir(tmp_dir, "some_bucket")
        assert os.path.exists(os.path.join(tmp_dir, "some_bucket"))
        prod_data_lake = ProductionHomogenousDataLake(bucket)
        prod_data_lake.store("a_table", "bar")

        assert os.path.exists(os.path.join(tmp_dir, "some_bucket", "a_table"))
        assert prod_data_lake.load("a_table") == "bar"
