from typing import Optional

from pydantic import BaseModel  # pylint: disable=E0611

from .kubernetes import (
    Affinity,
    Image,
    LivenessProbe,
    NodeSelector,
    PodSecurityContext,
    Resources,
    SecurityContext,
    Service,
    StartupProbe,
    Tolerations,
)
from .scheduler import Scheduler


class Dagit(BaseModel):
    replicaCount: int
    image: Image
    service: Service
    nodeSelector: Optional[NodeSelector]
    affinity: Optional[Affinity]
    tolerations: Optional[Tolerations]
    podSecurityContext: Optional[PodSecurityContext]
    securityContext: Optional[SecurityContext]
    resources: Optional[Resources]
    livenessProbe: Optional[LivenessProbe]
    startupProbe: Optional[StartupProbe]


class HelmValues(BaseModel):
    """
    Schema for Helm values.
    """

    dagit: Dagit
    scheduler: Scheduler
