from functools import lru_cache

import graphene
import yaml
from dagster import check
from dagster.core.host_representation import (
    ExternalPipeline,
    ExternalPresetData,
    RepresentedPipeline,
)
from dagster.core.snap import ConfigSchemaSnapshot, LoggerDefSnap, ModeDefSnap, ResourceDefSnap
from dagster.core.storage.pipeline_run import PipelineRunsFilter
from dagster.core.storage.tags import TagType, get_tag_type
from dagster_graphql.implementation.fetch_runs import get_runs
from dagster_graphql.implementation.fetch_schedules import get_schedules_for_pipeline
from dagster_graphql.implementation.fetch_sensors import get_sensors_for_pipeline
from dagster_graphql.implementation.utils import UserFacingGraphQLError, capture_error

from ..config_types import ConfigTypeField
from ..dagster_types import to_dagster_type
from ..errors import PipelineNotFoundError, PipelineSnapshotNotFoundError, PythonError
from ..runs import PipelineTag
from ..solids import SolidContainer, build_solid_handles, build_solids
from ..util import non_null_list


class PipelineReference(graphene.Interface):
    """This interface supports the case where we can look up a pipeline successfully in the
    repository available to the DagsterInstance/graphql context, as well as the case where we know
    that a pipeline exists/existed thanks to materialized data such as logs and run metadata, but
    where we can't look the concrete pipeline up."""

    name = graphene.NonNull(graphene.String)
    solidSelection = graphene.List(graphene.NonNull(graphene.String))


class UnknownPipeline(graphene.ObjectType):
    class Meta:
        interfaces = (PipelineReference,)

    name = graphene.NonNull(graphene.String)
    solidSelection = graphene.List(graphene.NonNull(graphene.String))
