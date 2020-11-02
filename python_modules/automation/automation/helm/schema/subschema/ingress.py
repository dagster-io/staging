from typing import List, Union

from pydantic import BaseModel  # pylint: disable=E0611

from . import kubernetes


# TODO: replace IngressPath with io.k8s.api.extensions.v1beta1.HTTPIngressPath as $ref type
class IngressPath(BaseModel):
    path: str
    serviceName: str
    servicePort: Union[str, int]


class DagitIngressConfiguration(BaseModel):
    host: str
    precedingPaths: List[IngressPath]
    succeedingPaths: List[IngressPath]


class FlowerIngressConfiguration(BaseModel):
    host: str
    path: str
    precedingPaths: List[IngressPath]
    succeedingPaths: List[IngressPath]


class Ingress(BaseModel):
    enabled: bool
    annotations: kubernetes.Annotations
    dagit: DagitIngressConfiguration
    flower: FlowerIngressConfiguration
