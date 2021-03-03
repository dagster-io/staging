from pydantic import BaseModel, Field  # pylint: disable=E0611

from .kubernetes import ImageWithRegistry


class RabbitMQConfiguration(BaseModel):
    username: str
    password: str


class Service(BaseModel):
    port: int


class VolumePermissions(BaseModel):
    enabled: bool = Field(default=True, const=True)
    image: ImageWithRegistry

class RabbitMQ(BaseModel):
    enabled: bool
    image: ImageWithRegistry
    rabbitmq: RabbitMQConfiguration
    service: Service
    volumePermissions: VolumePermissions
