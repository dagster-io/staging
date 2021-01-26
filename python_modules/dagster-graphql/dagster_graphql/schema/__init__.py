import graphene

from .roots.mutation import Mutation
from .roots.query import Query
from .roots.subscription import Subscription


def create_schema():
    from .logs import types as log_types
    from .pipelines import types as pipelines_types
    from .roots import types as roots_types
    from .asset_key import AssetKey
    from .backfill import PartitionBackfillResult, PartitionBackfillSuccess
    from .config_type_or_error import ConfigTypeOrError
    from .config_types import types as config_types
    from .dagster_types import types as dagster_types_types
    from .errors import types as errors_types
    from .execution import types as execution_types
    from .external import types as external_types
    from .inputs import types as inputs_types
    from .instance import DaemonHealth, DaemonStatus, Instance, RunLauncher
    from .jobs import types as jobs_types
    from .metadata import MetadataItemDefinition
    from .paging import Cursor
    from .partition_sets import types as partition_sets_types
    from .repository_origin import RepositoryOrigin, RepositoryMetadata
    from .run_config import RunConfigSchema, RunConfigSchemaOrError
    from .runs import types as runs_types
    from .sensors import types as sensors_types
    from .solids import types as solids_types
    from .tags import PipelineTag, PipelineTagAndValues
    from .used_solid import SolidInvocationSite, UsedSolid

    return graphene.Schema(
        query=Query,
        mutation=Mutation,
        subscription=Subscription,
        types=(
            log_types()
            + pipelines_types()
            + roots_types()
            + [AssetKey]
            + [PartitionBackfillResult, PartitionBackfillSuccess]
            + [ConfigTypeOrError]
            + config_types
            + dagster_types_types
            + errors_types
            + execution_types
            + external_types
            + inputs_types
            + [DaemonHealth, DaemonStatus, Instance, RunLauncher]
            + jobs_types
            + [MetadataItemDefinition]
            + [Cursor]
            + partition_sets_types
            + [RepositoryOrigin, RepositoryMetadata]
            + [RunConfigSchema, RunConfigSchemaOrError]
            + runs_types
            + sensors_types
            + solids_types
            + [PipelineTag, PipelineTagAndValues]
            + [SolidInvocationSite, UsedSolid]
        ),
    )
