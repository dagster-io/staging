import json

from dagster import AssetKey

from ..schema.node import GrapheneNodes
from ..schema.pipelines.pipeline import GrapheneAsset, GraphenePipeline
from ..schema.schedules.schedules import GrapheneSchedule
from ..schema.sensors import GrapheneSensor
from .fetch_pipelines import get_external_pipeline_or_raise
from .utils import PipelineSelector, capture_error


def _resolve_node(graphene_info, params):
    node_type = params.get("nodeType")
    name = params.get("name")
    repository_name = params.get("repositoryName")
    location_name = params.get("repositoryLocationName")

    if node_type == "pipeline":
        return GraphenePipeline(
            get_external_pipeline_or_raise(
                graphene_info, PipelineSelector(location_name, repository_name, name, None)
            )
        )
    elif node_type == "schedule":
        location = graphene_info.context.get_repository_location(location_name)
        repository = location.get_repository(repository_name)
        external_schedule = repository.get_external_schedule(name)
        return GrapheneSchedule(graphene_info, external_schedule=external_schedule)
    elif node_type == "sensor":
        location = graphene_info.context.get_repository_location(location_name)
        repository = location.get_repository(repository_name)
        external_sensor = repository.get_external_sensor(name)
        return GrapheneSensor(graphene_info, external_sensor=external_sensor)

    elif node_type == "asset":
        return GrapheneAsset(AssetKey(json.loads(name)))


@capture_error
def fetch_nodes(graphene_info, param_list):
    return GrapheneNodes(nodes=[_resolve_node(graphene_info, param) for param in param_list])
