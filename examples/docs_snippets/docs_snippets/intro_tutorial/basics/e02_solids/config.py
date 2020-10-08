import csv
import os

from dagster import Bool, Field, execute_pipeline, pipeline, solid


@solid
def read_csv(context, csv_path):
    csv_path = os.path.join(os.path.dirname(__file__), csv_path)
    with open(csv_path, "r") as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info("Read {n_lines} lines".format(n_lines=len(lines)))
    return lines


# start_config_marker_0
@solid(
    config_schema={
        "reverse": Field(
            Bool,
            default_value=False,
            is_required=False,
            description=(
                "If `True`, cereals will be sorted in reverse order. Default: `False`"
            ),
        )
    }
)
def sort_by_calories(context, cereals):
    sorted_cereals = sorted(
        cereals,
        key=lambda cereal: int(cereal["calories"]),
        reverse=context.solid_config["reverse"],
    )

    context.log.info(
        "Least caloric cereal: {least_caloric}".format(
            least_caloric=sorted_cereals[0]["name"]
        )
    )
    context.log.info(
        "Most caloric cereal: {most_caloric}".format(
            most_caloric=sorted_cereals[-1]["name"]
        )
    )
    return {
        "least_caloric": sorted_cereals[0],
        "most_caloric": sorted_cereals[-1],
    }


# end_config_marker_0
@pipeline
def config_pipeline():
    sort_by_calories(read_csv())


if __name__ == "__main__":
    # start_config_marker_1
    run_config = {
        "solids": {
            "read_csv": {"inputs": {"csv_path": {"value": "cereal.csv"}}},
            "sort_by_calories": {"config": {"reverse": False}},
        }
    }
    # end_config_marker_1

    # start_config_marker_2
    result = execute_pipeline(config_pipeline, run_config=run_config)
    # end_config_marker_2
    assert result.success
