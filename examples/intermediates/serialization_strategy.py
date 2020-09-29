import csv
import os

from dagster import SerializationStrategy, execute_pipeline, pipeline, solid, usable_as_dagster_type


class CsvSerializationStrategy(SerializationStrategy):
    def __init__(self):
        super(CsvSerializationStrategy, self).__init__(
            "csv_strategy", read_mode="r", write_mode="w"
        )

    def serialize(self, value, write_file_obj):
        fieldnames = value[0]
        writer = csv.DictWriter(write_file_obj, fieldnames)
        writer.writeheader()
        writer.writerows(value)

    def deserialize(self, read_file_obj):
        reader = csv.DictReader(read_file_obj)
        return MyDataFrame([row for row in reader])


@usable_as_dagster_type(
    name="MyDataFrame", serialization_strategy=CsvSerializationStrategy(),
)
class MyDataFrame(list):
    pass


@solid
def read_csv(context, csv_path: str) -> MyDataFrame:
    csv_path = os.path.join(os.path.dirname(__file__), csv_path)
    with open(csv_path, "r") as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info("Read {n_lines} lines".format(n_lines=len(lines)))
    return MyDataFrame(lines)


@solid
def sort_by_calories(_, cereals: MyDataFrame):
    sorted_cereals = sorted(cereals, key=lambda cereal: cereal["calories"])
    return MyDataFrame(sorted_cereals)


@pipeline
def serialization_strategy_pipeline():
    sort_by_calories(read_csv())


if __name__ == "__main__":
    run_config = {
        "solids": {"read_csv": {"inputs": {"csv_path": {"value": "data/cereal.csv"}}}},
        "storage": {"filesystem": {}},
    }
    result = execute_pipeline(serialization_strategy_pipeline, run_config=run_config)
    assert result.success
