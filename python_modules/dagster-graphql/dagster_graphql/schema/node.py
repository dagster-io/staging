import graphene

from .errors import GraphenePythonError
from .pipelines.pipeline import GrapheneAsset, GraphenePipeline
from .schedules.schedules import GrapheneSchedule
from .sensors import GrapheneSensor
from .util import non_null_list


class GrapheneNode(graphene.Union):
    class Meta:
        types = (
            GrapheneAsset,
            GraphenePipeline,
            GrapheneSensor,
            GrapheneSchedule,
        )


class GrapheneNodes(graphene.ObjectType):
    nodes = non_null_list(GrapheneNode)

    class Meta:
        name = "Nodes"


class GrapheneNodesOrError(graphene.Union):
    class Meta:
        types = (
            GrapheneNodes,
            GraphenePythonError,
        )
