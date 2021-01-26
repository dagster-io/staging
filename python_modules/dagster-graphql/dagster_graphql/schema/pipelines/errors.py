from collections import namedtuple

import graphene
from dagster import check
from dagster.core.host_representation import RepresentedPipeline
from dagster.core.snap import ConfigSchemaSnapshot

from ..config_types import ConfigTypeField
from ..errors import (
    Error,
    FieldNotDefinedErrorData,
    FieldsNotDefinedErrorData,
    MissingFieldErrorData,
    MissingFieldsErrorData,
    PipelineNotFoundError,
    PythonError,
    RuntimeMismatchErrorData,
    SelectorTypeErrorData,
    SerializableErrorInfo,
)
from ..util import non_null_list


class AssetsNotSupportedError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)


class AssetNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    def __init__(self, asset_key):
        self.asset_key = check.inst_param(asset_key, "asset_key", DagsterAssetKey)
        self.message = "Asset key {asset_key} not found.".format(asset_key=asset_key.to_string())

