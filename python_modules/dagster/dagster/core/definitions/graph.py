from .i_solid_definition import ISolidDefinition
from .solid_container import IContainSolids


# pylint: disable=abstract-method
class GraphDefinition(ISolidDefinition, IContainSolids):
    pass
