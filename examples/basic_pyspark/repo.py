# import warnings

from dagster_pyspark import DataFrame as DagsterPySparkDataFrame
from dagster_pyspark import pyspark_resource
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from dagster import (
    ModeDefinition,
    Output,
    execute_pipeline,
    make_python_type_usable_as_dagster_type,
    pipeline,
    repository,
    solid,
)
from dagster.core.definitions.events import Address

# Make pyspark.sql.DataFrame map to dagster_pyspark.DataFrame
make_python_type_usable_as_dagster_type(python_type=DataFrame, dagster_type=DagsterPySparkDataFrame)


@solid(required_resource_keys={"pyspark"})
def make_people(context) -> DataFrame:
    schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
    rows = [Row(name="Thom", age=51), Row(name="Jonny", age=48), Row(name="Nigel", age=49)]
    return Output(
        context.resources.pyspark.spark_session.createDataFrame(rows, schema),
        address=Address(
            path="/tmp/make_people.csv", spec={"csv": {"path": "/tmp/make_people", "header": True}}
        ),
    )


@solid
def filter_over_50(_, people: DataFrame) -> DataFrame:
    return people.filter(people["age"] > 50)


@solid
def count_people(_, people: DataFrame) -> int:
    return people.count()


@pipeline(mode_defs=[ModeDefinition(resource_defs={"pyspark": pyspark_resource})])
def my_pipeline():
    count_people(filter_over_50(make_people()))


@repository
def basic_pyspark_repo():
    return [my_pipeline]


# if __name__ == "__main__":
#     warnings.filterwarnings("ignore")
#     result = execute_pipeline(my_pipeline, {"storage": {"filesystem": None}})
#     print("----------make_people-------------------")
#     print("make_people", result.result_for_solid("make_people").output_value())
#     print("------------filter_over_50-----------------")
#     print("filter_over_50", result.result_for_solid("filter_over_50").output_value())
#     print("----------count_people-------------------")
#     print("count_people", result.result_for_solid("count_people").output_value())
