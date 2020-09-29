from dagster_pyspark import DataFrame as DagsterPySparkDataFrame
from dagster_pyspark import pyspark_resource
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from dagster import (
    DagsterInstance,
    ModeDefinition,
    Output,
    PresetDefinition,
    execute_pipeline,
    make_python_type_usable_as_dagster_type,
    pipeline,
    reexecute_pipeline,
    repository,
    solid,
)
from dagster.core.definitions.address import Address

# Make pyspark.sql.DataFrame map to dagster_pyspark.DataFrame
make_python_type_usable_as_dagster_type(python_type=DataFrame, dagster_type=DagsterPySparkDataFrame)


@solid(
    required_resource_keys={"pyspark"}, config_schema={"output_config": dict},
)
def make_people(context) -> DataFrame:
    schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
    rows = [Row(name="Thom", age=51), Row(name="Jonny", age=48), Row(name="Nigel", age=49)]
    return Output(
        value=context.resources.pyspark.spark_session.createDataFrame(rows, schema),
        address=Address(config_value=context.solid_config["output_config"]),
    )


@solid(
    required_resource_keys={"pyspark"}, config_schema={"output_config": dict},
)
def filter_over_50(context, people: DataFrame) -> DataFrame:
    return Output(
        value=people.filter(people["age"] > 50),
        address=Address(config_value=context.solid_config["output_config"]),
    )


@solid
def count_people(_, people: DataFrame) -> int:
    return people.count()


@pipeline(
    mode_defs=[
        ModeDefinition(name="local", resource_defs={"pyspark": pyspark_resource}),
        ModeDefinition(name="prod", resource_defs={"pyspark": pyspark_resource}),
    ],
    preset_defs=[
        PresetDefinition(
            name="local",
            mode="local",
            run_config={
                "solids": {
                    "make_people": {
                        "config": {
                            "output_config": {
                                "csv": {
                                    "path": "/tmp/make_people",
                                    "header": True,
                                    "mode": "overwrite",
                                }
                            }
                        }
                    },
                    "filter_over_50": {
                        "config": {
                            "output_config": {
                                "csv": {
                                    "path": "/tmp/filter_over_50",
                                    "header": True,
                                    "mode": "overwrite",
                                }
                            }
                        }
                    },
                },
                "storage": {"filesystem": None},
            },
        ),
        PresetDefinition(
            name="prod",
            mode="prod",
            run_config={
                "solids": {
                    "make_people": {
                        "config": {
                            "output_config": {"parquet": {"path": "s3://<bucket>/make_people",}}
                        }
                    },
                    "filter_over_50": {
                        "config": {
                            "output_config": {"parquet": {"path": "s3://<bucket>/filter_over_50",}}
                        }
                    },
                },
                "storage": {"filesystem": None},
            },
        ),
    ],
)
def my_pipeline():
    count_people(filter_over_50(make_people()))


@repository
def basic_pyspark_repo():
    return [my_pipeline]


if __name__ == "__main__":
    instance = DagsterInstance.ephemeral()
    result = execute_pipeline(my_pipeline, preset="local", instance=instance)
    reexecute_pipeline(
        my_pipeline,
        preset="local",
        instance=instance,
        parent_run_id=result.run_id,
        step_selection=["filter_over_50.compute*"],
    )
