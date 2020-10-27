from enum import Enum

from pydantic import BaseModel  # pylint: disable=E0611

from .utils import create_definition_ref


class PullPolicy(str, Enum):
    ALWAYS = "Always"
    IF_NOT_PRESENT = "IfNotPresent"
    NEVER = "Never"


class Image(BaseModel):
    repository: str
    tag: str
    pullPolicy: PullPolicy


class Kubernetes(BaseModel):
    image: Image


class Service(BaseModel):
    type: str
    port: int


class NodeSelector(BaseModel):
    class Config:
        schema_extra = {
            "$ref": create_definition_ref("io.k8s.api.core.v1.PodSpec/properties/nodeSelector")
        }


class Affinity(BaseModel):
    class Config:
        schema_extra = {"$ref": create_definition_ref("io.k8s.api.core.v1.Affinity")}


class Tolerations(BaseModel):
    class Config:
        schema_extra = {
            "$ref": create_definition_ref("io.k8s.api.core.v1.PodSpec/properties/tolerations")
        }


class PodSecurityContext(BaseModel):
    class Config:
        schema_extra = {"$ref": create_definition_ref("io.k8s.api.core.v1.PodSecurityContext")}


class SecurityContext(BaseModel):
    class Config:
        schema_extra = {"$ref": create_definition_ref("io.k8s.api.core.v1.SecurityContext")}


class Resources(BaseModel):
    class Config:
        schema_extra = {"$ref": create_definition_ref("io.k8s.api.core.v1.ResourceRequirements")}


class LivenessProbe(BaseModel):
    class Config:
        schema_extra = {
            "$ref": create_definition_ref("io.k8s.api.core.v1.Container/properties/livenessProbe")
        }


class StartupProbe(BaseModel):
    class Config:
        schema_extra = {
            "$ref": create_definition_ref(
                "io.k8s.api.core.v1.Container/properties/startupProbe", version="1.16.0"
            )
        }


class SecretEnvSource(BaseModel):
    class Config:
        schema_extra = {"$ref": create_definition_ref("io.k8s.api.core.v1.SecretEnvSource")}
