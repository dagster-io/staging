import csv

from dagster import (
    AssetMaterialization,
    dagster_type_loader,
    dagster_type_materializer,
    execute_pipeline,
    pipeline,
    solid,
    usable_as_dagster_type,
)


@dagster_type_loader({"path": str})
def my_loader(context, config_value):
    path = config_value["path"]
    with open(path, "r") as fd:
        lines = [row for row in csv.DictReader(fd)]
    context.log.info("USING LOADER: Read {n_lines} lines".format(n_lines=len(lines)))
    return MyDataFrame(lines)


@dagster_type_materializer({"path": str})
def my_materializer(context, config_value, runtime_value):
    path = config_value["path"]
    with open(path, "w") as fd:
        writer = csv.DictWriter(fd, fieldnames=runtime_value[0].keys())
        writer.writeheader()
        writer.writerows(rowdicts=runtime_value)
    context.log.info("USING MATERIALIZER: wrote {n_lines} lines".format(n_lines=len(runtime_value)))
    return AssetMaterialization.file(path)


@usable_as_dagster_type(name="MyDataFrame", loader=my_loader, materializer=my_materializer)
class MyDataFrame(list):
    pass


@solid
def sample_data(context, data: MyDataFrame) -> MyDataFrame:
    context.log.info(f"get {len(data)} rows")
    return MyDataFrame(data[:10])
    # or pass in dynamically generated path
    # return Output(
    #     value=MyDataFrame(data[:10]),
    #     address=Address(config_value={"path": "..."}),
    # )


@solid
def sort_by_calories(context, data: MyDataFrame) -> MyDataFrame:
    context.log.info(f"get {len(data)} rows")
    sorted_data = sorted(data, key=lambda row: row["calories"])
    return MyDataFrame(sorted_data)


@pipeline
def loader_materializer_pipeline():
    sort_by_calories(sample_data())


if __name__ == "__main__":
    run_config = {
        "solids": {
            "sample_data": {
                "inputs": {"data": {"path": "data/cereal.csv"}},
                # sample_data will output the result to "intermediates/cereal_sample.pickle" which will
                # part of the intermediate storage and will be passed by reference between solids
                "outputs": [{"result": {"path": "uncommitted/intermediates/cereal_sample.csv"}}],
            },
            # sort_by_calories will output the result to "output/result.csv"
            "sort_by_calories": {
                "outputs": [{"result": {"path": "uncommitted/output/result.csv"}}]
            },
        },
        "storage": {"filesystem": {}},
    }
    result = execute_pipeline(loader_materializer_pipeline, run_config=run_config)
    assert result.success
