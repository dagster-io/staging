import sys
import warnings

from dagster import repository
from dagster.utils import check_python_version


def get_event_pipelines():
    from dagster_examples.event_pipeline_demo.pipelines import event_ingest_pipeline

    return [event_ingest_pipeline]


def get_pyspark_pipelines():
    from dagster_examples.simple_pyspark.pipelines import simple_pyspark_sfo_weather_pipeline

    return [simple_pyspark_sfo_weather_pipeline]


def get_lakehouse_pipelines():
    from dagster_examples.simple_lakehouse.pipelines import simple_lakehouse_pipeline

    return [simple_lakehouse_pipeline]


def get_bay_bikes_pipelines():
    from dagster_examples.bay_bikes.pipelines import (
        daily_weather_pipeline,
        generate_training_set_and_train_model,
    )

    return [generate_training_set_and_train_model, daily_weather_pipeline]


@repository
def legacy_examples():
    pipelines = get_bay_bikes_pipelines() + get_event_pipelines()

    # Don't load the following pipelines in py38
    # https://github.com/dagster-io/dagster/issues/1960
    if not check_python_version(3, 8):
        pipelines += get_pyspark_pipelines() + get_lakehouse_pipelines()
    else:
        warnings.warn("Not loading pyspark pipelines because you are running py3.8")

    return pipelines
