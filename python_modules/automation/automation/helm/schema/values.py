from pydantic import BaseModel  # pylint: disable=E0611

from .dagit import Dagit
from .scheduler import Scheduler


class HelmValues(BaseModel):
    """
    Schema for Helm values.
    """

    dagit: Dagit
    scheduler: Scheduler
