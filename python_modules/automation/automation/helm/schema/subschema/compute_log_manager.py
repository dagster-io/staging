from enum import Enum
from typing import Any, Dict, Optional, Type

from pydantic import Extra  # pylint: disable=E0611

from .utils import BaseModel, create_json_schema_conditionals


class ComputeLogManagerType(str, Enum):
    AZURE = "AzureBlobComputeLogManager"
    GCS = "GCSComputeLogManager"
    S3 = "S3ComputeLogManager"
    CUSTOM = "CustomComputeLogManager"


class AzureBlobComputeLogManager(BaseModel):
    storageAccount: str
    container: str
    secretKey: str
    localDir: Optional[str]
    prefix: Optional[str]

    class Config:
        extra = Extra.forbid


class GCSComputeLogManager(BaseModel):
    bucket: str
    localDir: Optional[str]
    prefix: Optional[str]

    class Config:
        extra = Extra.forbid


class S3ComputeLogManager(BaseModel):
    bucket: str
    localDir: Optional[str]
    prefix: Optional[str]
    useSsl: Optional[bool]
    verify: Optional[bool]
    verifyCertPath: Optional[str]
    endpointUrl: Optional[str]

    class Config:
        extra = Extra.forbid


class CustomComputeLogManager(BaseModel):
    module: str
    class_: str
    config: dict

    class Config:
        fields = {"class_": "class"}
        extra = Extra.forbid


class ComputeLogManagerConfig(BaseModel):
    azureBlobComputeLogManager: Optional[AzureBlobComputeLogManager]
    gcsComputeLogManager: Optional[GCSComputeLogManager]
    s3ComputeLogManager: Optional[S3ComputeLogManager]
    customComputeLogManager: Optional[CustomComputeLogManager]

    class Config:
        extra = Extra.forbid


class ComputeLogManager(BaseModel):
    type: Optional[ComputeLogManagerType]
    config: ComputeLogManagerConfig

    class Config:
        extra = Extra.forbid

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['ComputeLogManager']):
            BaseModel.Config.schema_extra(schema, model)
            schema["allOf"] = create_json_schema_conditionals(
                {
                    ComputeLogManagerType.AZURE: "azureBlobComputeLogManager",
                    ComputeLogManagerType.GCS: "gcsComputeLogManager",
                    ComputeLogManagerType.S3: "s3ComputeLogManager",
                    ComputeLogManagerType.CUSTOM: "customComputeLogManager",
                }
            )
