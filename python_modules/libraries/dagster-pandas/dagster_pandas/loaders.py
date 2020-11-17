import pandas as pd
from dagster import Field, StringSource
from dagster.core.types.loader_entry import loader_from_entries, type_loader_entry
from dagster.utils import dict_without_keys


@type_loader_entry(
    config_schema={
        "path": StringSource,
        "sep": Field(StringSource, is_required=False, default_value=","),
    },
)
def csv(_, file_options):
    path = file_options["path"]
    return pd.read_csv(path, **dict_without_keys(file_options, "path"))


@type_loader_entry({"path": StringSource})
def parquet(_, file_options):
    return pd.read_parquet(file_options["path"])


@type_loader_entry({"path": StringSource})
def table(_, file_options):
    return pd.read_csv(file_options["path"], sep="\t")


@type_loader_entry({"path": StringSource})
def pickle(_, file_options):
    return pd.read_pickle(file_options["path"])


def default_pandas_df_loader():
    return loader_from_entries([csv, parquet, table, pickle])
