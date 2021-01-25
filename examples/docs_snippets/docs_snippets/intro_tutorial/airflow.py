import csv

from dagster import pipeline, solid
from dagster.utils import file_relative_path


@solid
def hello_cereal(context):
    dataset_path = file_relative_path(__file__, "cereal.csv")
    context.log.info(dataset_path)
    with open(dataset_path, "r") as fd:
        cereals = [row for row in csv.DictReader(fd)]

    context.log.info(f"Found {len(cereals)} cereals")


@pipeline
def hello_cereal_pipeline():
    hello_cereal()
