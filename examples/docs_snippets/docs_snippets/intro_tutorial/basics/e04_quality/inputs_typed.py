import csv
import os

from dagster import execute_pipeline, pipeline, solid


# start_a30dee98029e11ebaf72acde48001122
# start_a30da102029e11eb8aadacde48001122
@solid
def read_csv(context, csv_path: str):
    csv_path = os.path.join(os.path.dirname(__file__), csv_path)
    with open(csv_path, "r") as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info("Read {n_lines} lines".format(n_lines=len(lines)))
    return lines


# end_a30dee98029e11ebaf72acde48001122

# end_a30da102029e11eb8aadacde48001122


@solid
def sort_by_calories(context, cereals):
    sorted_cereals = sorted(cereals, key=lambda cereal: cereal["calories"])
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


@pipeline
def inputs_pipeline():
    sort_by_calories(read_csv())


if __name__ == "__main__":
    run_config = {
        "solids": {
            "read_csv": {"inputs": {"csv_path": {"value": "cereal.csv"}}}
        }
    }
    result = execute_pipeline(inputs_pipeline, run_config=run_config)
    assert result.success
