from typing import Dict

from dagster import (
    DagsterType,
    InputDefinition,
    ModeDefinition,
    ObjectManager,
    OutputDefinition,
    TypeCheck,
    object_manager,
    pipeline,
    repository,
    solid,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DataType, LongType, StringType, StructField, StructType


@object_manager
def local_parquet_store(_):
    raise NotImplementedError()


def cols_to_struct_type(cols: Dict[str, DataType]) -> StructType:
    return StructType([StructField(name, spark_type) for name, spark_type in cols.items()])


class SparkDFType(DagsterType):
    def __init__(self, cols: Dict[str, DataType]):
        self.cols = cols

        def type_check_fn(_, value):
            """Verify that the given dataframe's columns match the type's columns"""
            for actual_field, expected_field in zip(value.schema, cols_to_struct_type(cols)):
                if (
                    actual_field.name != expected_field.name
                    or actual_field.dataType != expected_field.dataType
                ):
                    return TypeCheck(False, f"{actual_field} != {expected_field}")

            return True

        super(SparkDFType, self).__init__(type_check_fn=type_check_fn, name=str(cols))


class SmokeTestObjectManager(ObjectManager):
    def handle_output(self, _, obj):
        """Triggers computation of the provided Spark DataFrame."""
        obj.count()

    def load_input(self, context):
        """Creates an empty Spark DataFrame with the expected columns."""
        spark = SparkSession.builder.getOrCreate()
        if context.upstream_output:
            cols = context.upstream_output.dagster_type.cols
        else:
            cols = context.dagster_type.cols
        return spark.createDataFrame([], cols_to_struct_type(cols))


@object_manager
def smoke_test_object_manager(_):
    return SmokeTestObjectManager()


@solid(
    input_defs=[
        InputDefinition(
            "people",
            dagster_type=SparkDFType({"name": StringType(), "age": LongType()}),
            manager_key="object_manager",
        )
    ],
    output_defs=[
        OutputDefinition(SparkDFType({"name": StringType(), "age_bracket": StringType()}))
    ],
)
def people_with_age_brackets(_, people):
    """Buckets people based on their age."""
    return people.select(
        "name",
        f.when(f.col("age") > 50, f.lit("> 50")).otherwise(f.lit("<= 50")).alias("age_bracket"),
    )


@solid(
    output_defs=[
        OutputDefinition(SparkDFType({"age_bracket": StringType(), "num_people": LongType()}))
    ]
)
def count_by_age(_, people):
    """Counts the number of people in each age bracket."""
    return people.groupBy("age_bracket").agg(f.count("name").alias("num_people"))


@pipeline(
    mode_defs=[
        ModeDefinition("local", resource_defs={"object_manager": local_parquet_store}),
        ModeDefinition("smoke_test", resource_defs={"object_manager": smoke_test_object_manager}),
    ]
)
def my_pipeline():
    count_by_age(people_with_age_brackets())


@repository
def pyspark_smoke_test_repo():
    return [my_pipeline]
