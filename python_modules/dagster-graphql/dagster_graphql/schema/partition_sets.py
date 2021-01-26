import graphene
from dagster import check
from dagster.core.host_representation import ExternalPartitionSet, RepositoryHandle
from dagster.core.storage.pipeline_run import PipelineRunsFilter as DagsterPipelineRunsFilter
from dagster.core.storage.tags import PARTITION_NAME_TAG, PARTITION_SET_TAG
from dagster.utils import merge_dicts
from dagster_graphql.implementation.fetch_partition_sets import (
    get_partition_by_name,
    get_partition_config,
    get_partition_set_partition_statuses,
    get_partition_tags,
    get_partitions,
)
from dagster_graphql.implementation.fetch_runs import get_runs

from .errors import PartitionSetNotFoundError, PipelineNotFoundError, PythonError
from .inputs import PipelineRunsFilter
from .pipelines.pipeline import PipelineRun
from .pipelines.status import PipelineRunStatus
from .tags import PipelineTag
from .util import non_null_list


class PartitionTags(graphene.ObjectType):
    results = non_null_list(PipelineTag)


class PartitionRunConfig(graphene.ObjectType):
    yaml = graphene.NonNull(graphene.String)


class PartitionRunConfigOrError(graphene.Union):
    class Meta:
        types = (PartitionRunConfig, PythonError)


class PartitionStatus(graphene.ObjectType):
    id = graphene.NonNull(graphene.String)
    partitionName = graphene.NonNull(graphene.String)
    runStatus = graphene.Field(PipelineRunStatus)


class PartitionStatuses(graphene.ObjectType):
    results = non_null_list(PartitionStatus)


class PartitionStatusesOrError(graphene.Union):
    class Meta:
        types = (PartitionStatuses, PythonError)


class PartitionTagsOrError(graphene.Union):
    class Meta:
        types = (PartitionTags, PythonError)


class Partition(graphene.ObjectType):
    name = graphene.NonNull(graphene.String)
    partition_set_name = graphene.NonNull(graphene.String)
    solid_selection = graphene.List(graphene.NonNull(graphene.String))
    mode = graphene.NonNull(graphene.String)
    runConfigOrError = graphene.NonNull(PartitionRunConfigOrError)
    tagsOrError = graphene.NonNull(PartitionTagsOrError)
    runs = graphene.Field(
        non_null_list(PipelineRun),
        filter=graphene.Argument(PipelineRunsFilter),
        cursor=graphene.String(),
        limit=graphene.Int(),
    )
    status = graphene.Field(PipelineRunStatus)

    def __init__(self, external_repository_handle, external_partition_set, partition_name):
        self._external_repository_handle = check.inst_param(
            external_repository_handle, "external_respository_handle", RepositoryHandle
        )
        self._external_partition_set = check.inst_param(
            external_partition_set, "external_partition_set", ExternalPartitionSet
        )
        self._partition_name = check.str_param(partition_name, "partition_name")

        super(Partition, self).__init__(
            name=partition_name,
            partition_set_name=external_partition_set.name,
            solid_selection=external_partition_set.solid_selection,
            mode=external_partition_set.mode,
        )

    def resolve_runConfigOrError(self, graphene_info):
        return get_partition_config(
            graphene_info,
            self._external_repository_handle,
            self._external_partition_set.name,
            self._partition_name,
        )

    def resolve_tagsOrError(self, graphene_info):
        return get_partition_tags(
            graphene_info,
            self._external_repository_handle,
            self._external_partition_set.name,
            self._partition_name,
        )

    def resolve_runs(self, graphene_info, **kwargs):
        filters = kwargs.get("filter")
        partition_tags = {
            PARTITION_SET_TAG: self._external_partition_set.name,
            PARTITION_NAME_TAG: self._partition_name,
        }
        if filters is not None:
            filters = filters.to_selector()
            runs_filter = DagsterPipelineRunsFilter(
                run_ids=filters.run_ids,
                pipeline_name=filters.pipeline_name,
                statuses=filters.statuses,
                tags=merge_dicts(filters.tags, partition_tags),
            )
        else:
            runs_filter = DagsterPipelineRunsFilter(tags=partition_tags)

        return get_runs(
            graphene_info, runs_filter, cursor=kwargs.get("cursor"), limit=kwargs.get("limit")
        )


class Partitions(graphene.ObjectType):
    results = non_null_list(Partition)


class PartitionsOrError(graphene.Union):
    class Meta:
        types = (Partitions, PythonError)


class PartitionSet(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    name = graphene.NonNull(graphene.String)
    pipeline_name = graphene.NonNull(graphene.String)
    solid_selection = graphene.List(graphene.NonNull(graphene.String))
    mode = graphene.NonNull(graphene.String)
    partitionsOrError = graphene.Field(
        graphene.NonNull(PartitionsOrError),
        cursor=graphene.String(),
        limit=graphene.Int(),
        reverse=graphene.Boolean(),
    )
    partition = graphene.Field(Partition, partition_name=graphene.NonNull(graphene.String))
    partitionStatusesOrError = graphene.NonNull(PartitionStatusesOrError)

    def __init__(self, external_repository_handle, external_partition_set):
        self._external_repository_handle = check.inst_param(
            external_repository_handle, "external_respository_handle", RepositoryHandle
        )
        self._external_partition_set = check.inst_param(
            external_partition_set, "external_partition_set", ExternalPartitionSet
        )

        super(PartitionSet, self).__init__(
            name=external_partition_set.name,
            pipeline_name=external_partition_set.pipeline_name,
            solid_selection=external_partition_set.solid_selection,
            mode=external_partition_set.mode,
        )

    def resolve_id(self, _graphene_info):
        return self._external_partition_set.get_external_origin_id()

    def resolve_partitionsOrError(self, graphene_info, **kwargs):
        return get_partitions(
            graphene_info,
            self._external_repository_handle,
            self._external_partition_set,
            cursor=kwargs.get("cursor"),
            limit=kwargs.get("limit"),
            reverse=kwargs.get("reverse"),
        )

    def resolve_partition(self, graphene_info, partition_name):
        return get_partition_by_name(
            graphene_info,
            self._external_repository_handle,
            self._external_partition_set,
            partition_name,
        )

    def resolve_partitionStatusesOrError(self, graphene_info):
        return get_partition_set_partition_statuses(
            graphene_info, self._external_repository_handle, self._external_partition_set.name
        )


class PartitionSetOrError(graphene.Union):
    class Meta:
        types = (PartitionSet, PartitionSetNotFoundError, PythonError)


class PartitionSets(graphene.ObjectType):
    results = non_null_list(PartitionSet)


class PartitionSetsOrError(graphene.Union):
    class Meta:
        types = (PartitionSets, PipelineNotFoundError, PythonError)


types = [
    Partition,
    PartitionRunConfig,
    PartitionRunConfigOrError,
    Partitions,
    PartitionSet,
    PartitionSetOrError,
    PartitionSets,
    PartitionSetsOrError,
    PartitionsOrError,
    PartitionStatus,
    PartitionStatuses,
    PartitionStatusesOrError,
    PartitionTags,
    PartitionTagsOrError,
]
