from typing import Dict

from dagster import (
    DagsterType,
    IOManager,
    ModeDefinition,
    OutputDefinition,
    SourceDefinition,
    TypeCheck,
    io_manager,
    pipeline,
    repository,
    solid,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DataType, LongType, StringType, StructField, StructType


@io_manager
def local_parquet_io_manager(_):
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


class SmokeTestIOManager(IOManager):
    def handle_output(self, _, obj):
        """Triggers computation of the provided Spark DataFrame."""
        obj.count()

    def load_input(self, context):
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(
            [], cols_to_struct_type(context.upstream_output.dagster_type.cols)
        )


@io_manager
def smoke_test_object_manager(_):
    return SmokeTestIOManager()


people_source = SourceDefinition(
    dagster_type=SparkDFType({"name": StringType(), "age": LongType()})
)


@solid(
    output_defs=[OutputDefinition(SparkDFType({"name": StringType(), "age_bracket": StringType()}))]
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
        ModeDefinition("local", resource_defs={"io_manager": local_parquet_io_manager}),
        ModeDefinition("smoke_test", resource_defs={"io_manager": smoke_test_object_manager}),
    ]
)
def my_pipeline():
    count_by_age(people_with_age_brackets(people_source))


@repository
def pyspark_smoke_test_repo():
    return [my_pipeline]
