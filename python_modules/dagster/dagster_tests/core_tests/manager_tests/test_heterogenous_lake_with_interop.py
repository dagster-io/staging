"""
This set of tests encapsulates a heterogenous situation where you are flowing
data through a bunch of different stores, potentially (but not necessarily)
controlled by different teams.

There are many cases where one cannot rely on upstream solids to provide
the information to load the input. 

1) You are using a pre-existing solid that has captured the externalities
using output definitions and instead has just dynamically generated the
assets. At best, you get an output that represents the metadata around
this operation. Often you'll only be dealing with Nothing input.

2) Another team has created the data lake layout with a given technology.
You do not want to modify that team's code but you want to consume both
their solids and their data in their data lake. You want to be able write
your own input loading routines without having unnecessary "stubs".

The generalized way of expressing these situations is where you are
writing solids where you either cannot or do not want to rely on the
upstream solid to determine loading behavior. This is conceivable in
scenarios where you are bringing in reusable solids from a library,
crossing team boundaries, or in the course of an incremental migration
where you do not fully understand or want to modify the legacy code upstream.

More concretely this example is data lake one --> dbt over snowflake --> data lake two.

One could imagine this, for example, during a migration process. 
"""

import re

import pytest
from dagster import InputDefinition as RealInputDefinition
from dagster import Nothing
from dagster import OutputDefinition as RealOutputDefinition
from dagster import PythonObjectDagsterType, pipeline, solid
from dagster.core.errors import DagsterInvalidDefinitionError

# e.g. a pyspark dataframe
DataLakeOneDataFrame = PythonObjectDagsterType(name="DataLakeOneDataFrame", python_type=str)

# e.g. a pandas dataframe
DataLakeTwoDataFrame = PythonObjectDagsterType(name="DataLakeTwoDataFrame", python_type=str)


def InputDefinition(
    *args, manager_key=None, metadata=None, **kwargs
):  # pylint: disable=unused-argument
    return RealInputDefinition(*args, **kwargs)


def OutputDefinition(
    *args, manager_key=None, metadata=None, **kwargs
):  # pylint: disable=unused-argument
    return RealOutputDefinition(*args, **kwargs)


@solid(
    input_defs=[InputDefinition("in_path", str)],
    output_defs=[
        OutputDefinition(
            DataLakeOneDataFrame,
            manager_key="data_lake_one",
            metadata={"coord_for_one": "data_lake_one_table_a"},
        )
    ],
)
def initial_ingest(_, in_path: str) -> str:
    return "df-from-" + in_path


@solid(
    input_defs=[InputDefinition("df", DataLakeTwoDataFrame)],
    output_defs=[
        OutputDefinition(
            DataLakeTwoDataFrame,
            manager_key="data_lake_two_to_snowflake",
            metadata={"table_name": "source_table_b"},
        )
    ],
)
def create_table_b(_, df: str) -> str:
    return df + "-table-b"


DbtOutputObject = PythonObjectDagsterType(name="DbtOutputObject", python_type=str)


@solid(
    input_defs=[InputDefinition("on_start", Nothing)],
    output_defs=[OutputDefinition(DbtOutputObject, "on_end")],
)
def fake_dbt_solid(_):
    return "dummy"


@solid(
    input_defs=[InputDefinition("on_start", Nothing)],
    output_defs=[OutputDefinition(Nothing, "on_end")],
)
def fake_dbt_solid_that_returns_nothing(_):
    pass


# Note: there is an argument that this is overkill, and that there
# should be a solid which has a nothing input and that loads the
# dataframe in question.
#
# However there are going to be cases where someone depends on an
# upstream solid that lies. E.g. it produces some sort of externality
# which is not captured by its output definitions.
#
# This can easily happen when one does not have control over their
# upstream solids for an organizational or technical reason
@solid(
    input_defs=[
        InputDefinition(
            "df",
            DataLakeTwoDataFrame,
            # team two has their own manager for reading from snowflake
            manager_key="snowflake_for_team_two",
            metadata={"table": "table_one"},
        )
    ],
    output_defs=[
        OutputDefinition(
            DataLakeTwoDataFrame,
            manager_key="data_lake_two",
            metadata={"coord_for_two": "data_lake_two_table_cc"},
        )
    ],
)
def create_table_cc(_, df: str) -> str:
    return df + "-processed"


@solid(
    input_defs=[
        InputDefinition(
            "df",
            DataLakeTwoDataFrame,
            manager_key="snowflake_for_lake_two",
            metadata={"table": "table_two"},
        )
    ],
    output_defs=[
        OutputDefinition(
            DataLakeTwoDataFrame,
            manager_key="data_lake_two",
            metadata={"coord_for_two": "data_lake_two_table_dd"},
        )
    ],
)
def create_table_dd(_, df: str) -> str:
    return df + "-processed"


def test_three_stage_pipeline_ctor():
    @pipeline
    def heterogenous_lake_with_interop():
        dbt_complete = fake_dbt_solid(initial_ingest())
        create_table_cc(dbt_complete)
        create_table_dd(dbt_complete)

    assert heterogenous_lake_with_interop


def test_pipeline_ctor_fails_on_nothing_dep():

    # Expected to fail because of the nothing to df connection
    # Imagine one did not have control over the dbt solid
    # and wanted to avoid have a stub solid that loaded a data frame
    with pytest.raises(
        DagsterInvalidDefinitionError,
        match=re.escape("returns type Nothing (which produces no value)"),
    ):

        @pipeline
        def heterogenous_lake_with_interop_with_nothing():
            dbt_complete = fake_dbt_solid_that_returns_nothing(initial_ingest())
            create_table_cc(dbt_complete)
            create_table_dd(dbt_complete)

        assert heterogenous_lake_with_interop_with_nothing


# These capture a simpler case where it is a single data lake stored
# but two different compute substrates. One wants to be able to author
# the consume_some_table solid without modifying create_some_table or
# the manager it references


@solid(
    output_defs=[
        OutputDefinition(
            DataLakeOneDataFrame,
            manager_key="data_lake_one",
            metadata={"coord_for_one": "some_table"},
        )
    ]
)
def create_some_table(_) -> str:
    return "dummy"


@solid(
    input_defs=[
        InputDefinition(
            "df",
            DataLakeTwoDataFrame,
            manager_key="data_lake_two",
            metadata={"coord_for_two": "some_table"},
        )
    ]
)
def consume_some_table(_, df):
    return df


def test_cross_the_dataframe_chasm():
    @pipeline
    def chasm_cross():
        consume_some_table(create_some_table())

    assert chasm_cross
