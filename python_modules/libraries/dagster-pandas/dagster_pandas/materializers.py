from dagster import AssetMaterialization, Field, StringSource
from dagster.core.types.loader_entry import materializer_from_entries, type_materializer_entry
from dagster.utils import dict_without_keys


@type_materializer_entry(
    {"path": StringSource, "sep": Field(StringSource, is_required=False, default_value=",")}
)
def csv(_, file_options, pandas_df):
    path = file_options["path"]
    pandas_df.to_csv(path, index=False, **dict_without_keys(file_options, "path"))
    return AssetMaterialization.file(file_options["path"])


@type_materializer_entry({"path": StringSource})
def parquet(_, file_options, pandas_df):
    pandas_df.to_parquet(file_options["path"])
    return AssetMaterialization.file(file_options["path"])


@type_materializer_entry({"path": StringSource})
def table(_, file_options, pandas_df):
    pandas_df.to_csv(file_options["path"], sep="\t", index=False)
    return AssetMaterialization.file(file_options["path"])


@type_materializer_entry({"path": StringSource})
def pickle(_, file_options, pandas_df):
    pandas_df.to_pickle(file_options["path"])
    return AssetMaterialization.file(file_options["path"])


def default_pandas_materializer():
    return materializer_from_entries([csv, parquet, table, pickle])
