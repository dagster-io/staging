from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .solid import SolidDefinition
    from .resource import ResourceDefinition


class VersionStrategy:
    @staticmethod
    def get_solid_version(solid_def: "SolidDefinition") -> str:
        raise NotImplementedError()

    @staticmethod
    def get_resource_version(resource_def: "ResourceDefinition") -> str:
        raise NotImplementedError()
