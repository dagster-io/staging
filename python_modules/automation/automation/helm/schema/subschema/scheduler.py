from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Extra  # pylint: disable=E0611

from .kubernetes import Image, SecretEnvSource


class SchedulerType(str, Enum):
    CRON_SCHEDULER = "CronScheduler"
    K8S_SCHEDULER = "K8sScheduler"


class K8sSchedulerConfig(BaseModel):
    image: Image
    schedulerNamespace: str
    loadInclusterConfig: Optional[bool]
    kubeconfigFile: Optional[str]
    envSecrets: Optional[List[SecretEnvSource]]

    class Config:
        extra = Extra.forbid


class SchedulerConfig(BaseModel):
    K8sScheduler: Optional[K8sSchedulerConfig]

    class Config:
        extra = Extra.forbid


class Scheduler(BaseModel):
    type: SchedulerType
    config: SchedulerConfig

    class Config:
        extra = Extra.forbid
        schema_extra = {
            "allOf": [
                {
                    "if": {"properties": {"type": {"const": SchedulerType.K8S_SCHEDULER}},},
                    "then": {"properties": {"config": {"required": [SchedulerType.K8S_SCHEDULER]}}},
                }
            ]
        }
