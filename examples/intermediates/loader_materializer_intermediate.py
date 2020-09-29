import csv
import pickle

from dagster import (
    AssetMaterialization,
    Output,
    Selector,
    check,
    dagster_type_loader,
    dagster_type_materializer,
    execute_pipeline,
    pipeline,
    solid,
    usable_as_dagster_type,
)
from dagster.core.definitions.address import Address


@dagster_type_loader(
    Selector(
        {
            # handles external inputs
            "original": str,
            # handles intermediates
            "pickle": str,
        }
    )
)
def my_loader(context, config_value):
    file_type, path = list(config_value.items())[0]
    if file_type == "original":
        with open(path, "r") as fd:
            lines = [row for row in csv.DictReader(fd)]
    elif file_type == "pickle":
        with open(path, "rb") as ff:
            lines = pickle.load(ff)
    else:
        check.failed("unknown loading type")

    context.log.info("Read {n_lines} lines".format(n_lines=len(lines)))
    return MyDataFrame(lines)


@dagster_type_materializer(
    Selector(
        {
            # handles human readable outputs
            "original": str,
            # handles intermediates
            "pickle": str,
        }
    )
)
def my_materializer(_, config_value, runtime_value):
    file_type, path = list(config_value.items())[0]

    if file_type == "original":
        with open(path, "w") as fd:
            writer = csv.DictWriter(fd, fieldnames=runtime_value[0].keys())
            writer.writeheader()
            writer.writerows(rowdicts=runtime_value)
    elif file_type == "pickle":
        with open(path, "wb") as ff:
            pickle.dump(runtime_value, ff)
    else:
        check.failed("unknown materializing type")

    return AssetMaterialization.file(path)


@usable_as_dagster_type(name="MyDataFrame", loader=my_loader, materializer=my_materializer)
class MyDataFrame(list):
    pass


@solid(config_schema={"output_config": dict})
def sample_data(context, data: MyDataFrame) -> MyDataFrame:
    context.log.info(f"get {len(data)} rows")
    # return MyDataFrame(data[:10])
    return Output(
        value=MyDataFrame(data[:10]),
        address=Address(config_value=context.solid_config["output_config"]),
    )


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
                "inputs": {"data": {"original": "data/cereal.csv"}},
                # sample_data will output the result to "intermediates/cereal_sample.pickle" which will
                # part of the intermediate storage and will be passed by reference between solids
                "config": {"output_config": {"pickle": "intermediates/cereal_sample.pickle"}},
            },
            # sort_by_calories will output the result to "output/result.csv" which won't be counted as an intermediate
            "sort_by_calories": {"outputs": [{"result": {"original": "output/result.csv"}}],},
        },
        "storage": {"filesystem": {}},
    }
    result = execute_pipeline(loader_materializer_pipeline, run_config=run_config)
    assert result.success
