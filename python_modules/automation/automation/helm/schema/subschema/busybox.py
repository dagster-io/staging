from pydantic import BaseModel

from .kubernetes import Image


class Busybox(BaseModel):
    image: Image
