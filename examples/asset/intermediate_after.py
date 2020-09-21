import csv
import os
from collections import OrderedDict
from datetime import datetime

from dagster import execute_pipeline, pipeline, solid


@solid
def raw_data(context, source_data_path):
    csv_path = os.path.join(os.path.dirname(__file__), source_data_path)

    with open(csv_path, "r") as fd:
        source_data = [row for row in csv.DictReader(fd)]

    context.log.info("Read from {path}".format(path=csv_path))

    # users only need to output the data object without worrying about how it's passed
    # and Dagster machinery would determine whether to pass its inline value or
    # its reference (e.g. path) between solids
    return source_data


@solid
def preprocess(_, source_data):
    # users don't have to handle the loading themselves. if it's passed by reference, Dagster
    # should handle the loading inside its machinery.
    processed = []
    for row in source_data:
        processed.append(
            OrderedDict(
                [
                    ("date", datetime.strptime(row["date"], "%d-%m-%Y")),
                    ("high", row["maximum temperature"]),
                    ("low", row["minimum temperature"]),
                ]
            )
        )
    # again, pass by reference
    return processed


@solid
def daily_highs(_, data):
    highs = []
    for row in data:
        highs.append(OrderedDict([("date", row["date"]), ("high", row["high"]),]))
    return sorted(highs, key=lambda x: x["date"])


@pipeline
def weather_pipeline_pass_by_ref():
    daily_highs(preprocess(raw_data()))


if __name__ == "__main__":
    run_config_local_file = {
        "solids": {
            "raw_data": {
                "inputs": {
                    "source_data_path": {"value": "data/weather_data_nyc_centralpark_2016.csv"}
                },
            },
        },
        "storage": {"filesystem": None},
    }

    result = execute_pipeline(weather_pipeline_pass_by_ref, run_config_local_file)
    intermediate_events = list(
        filter(lambda x: x.event_type_value == "OBJECT_STORE_OPERATION", result.event_list)
    )

    # TODO goal: output value == intermediate path if pass by reference
    assert (
        result.result_for_solid("raw_data").output_value()
        == intermediate_events[0].event_specific_data.metadata_entries[0].entry_data.path
    )
