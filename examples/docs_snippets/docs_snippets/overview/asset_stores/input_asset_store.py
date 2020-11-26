# pylint: disable=unused-argument

import os

from dagster import InputDefinition, ModeDefinition, pipeline, solid
from dagster.core.storage.asset_store import loader


def read_csv(_path):
    return 1


@loader()
def my_hardcoded_csv_loader(_context, _resource_config, _input_config):
    return read_csv("hard/coded/path")


@loader(config_schema={"base_dir": str}, input_config_schema={"key": str})
def my_configurable_csv_loader(_context, resource_config, input_config):
    path = os.path.join(resource_config["base_dir"], input_config["key"])
    return read_csv(path)


@solid(input_defs=[InputDefinition("input1", loader_key="my_loader")])
def solid1(_, input1):
    """Return a Pandas DataFrame"""


@pipeline(mode_defs=[ModeDefinition(resource_defs={"my_loader": my_hardcoded_csv_loader})])
def my_pipeline():
    solid1()


@pipeline(mode_defs=[ModeDefinition(resource_defs={"my_loader": my_configurable_csv_loader})])
def my_configurable_pipeline():
    solid1()
