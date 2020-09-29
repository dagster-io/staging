# start_a30bb018029e11ebae2bacde48001122
import csv
import os

from dagster import execute_pipeline, pipeline, solid

# start_a30fff38029e11ebaf70acde48001122


@solid
def hello_cereal(context):
    # Assuming the dataset is in the same directory as this file
    dataset_path = os.path.join(os.path.dirname(__file__), "cereal.csv")
    with open(dataset_path, "r") as fd:
        # Read the rows in using the standard csv library
        cereals = [row for row in csv.DictReader(fd)]

    context.log.info(
        "Found {n_cereals} cereals".format(n_cereals=len(cereals))
    )

    return cereals


# end_a30fff38029e11ebaf70acde48001122

# start_a30beff6029e11eba443acde48001122
# end_a30bb018029e11ebae2bacde48001122


@pipeline
# end_a30beff6029e11eba443acde48001122
def hello_cereal_pipeline():
    hello_cereal()


if __name__ == "__main__":
    result = execute_pipeline(hello_cereal_pipeline)
