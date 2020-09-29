import csv
import pickle

from dagster import (
    AssetMaterialization,
    Output,
    Selector,
    dagster_type_loader,
    dagster_type_materializer,
    execute_pipeline,
    pipeline,
    solid,
    usable_as_dagster_type,
)


@dagster_type_loader(Selector({"pickle": str}))
def my_loader(context, config_value):
    with open(config_value["pickle"], "r") as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info("Read {n_lines} lines".format(n_lines=len(lines)))
    return MyDataFrame(lines)


@dagster_type_materializer(Selector({"pickle": str}))
def my_materializer(_, config_value, runtime_value):
    file_type, path = list(config_value.items())[0]
    if file_type == "pickle":
        with open(path, "wb") as ff:
            pickle.dump(runtime_value, ff)
        return AssetMaterialization.file(path)


@usable_as_dagster_type(name="MyDataFrame", loader=my_loader, materializer=my_materializer)
class MyDataFrame(list):
    pass


@solid
def sample_data(context, data: MyDataFrame) -> MyDataFrame:
    context.log.info(f"get {len(data)} rows")
    return MyDataFrame(data[:10])


@solid
def sort_by_calories(context, data: MyDataFrame) -> MyDataFrame:
    context.log.info(f"get {len(data)} rows")
    sorted_data = sorted(data, key=lambda row: row["calories"])
    return Output(MyDataFrame(sorted_data))


@pipeline
def loader_materializer_pipeline():
    sort_by_calories(sample_data())


if __name__ == "__main__":
    run_config = {
        "solids": {
            "sample_data": {
                "inputs": {"data": {"pickle": "data/cereal.csv"}},
                "outputs": [{"result": {"pickle": "output/cereal_sample.csv"}}],
            },
            "sort_by_calories": {"outputs": [{"result": {"pickle": "output/result.csv"}}],},
        },
        # specify "outputs" will cause the data object persisted twice - one in materializer, one in intermediate
        # and intermediates are copied between runs
        "storage": {"filesystem": {}},
    }
    result = execute_pipeline(loader_materializer_pipeline, run_config=run_config)
    assert result.success
