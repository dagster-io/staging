from dagster_graphql import dauphin
from dagster_graphql.implementation.fetch_runs import get_runs
from dagster_graphql.implementation.fetch_triggers import (
    get_trigger_execution_config,
    get_trigger_execution_tags,
)
from dagster_graphql.schema.errors import (
    DauphinPythonError,
    DauphinRepositoryNotFoundError,
    DauphinTriggerDefinitionNotFoundError,
)

from dagster import check
from dagster.core.host_representation.external import ExternalTriggeredExecution
from dagster.core.storage.pipeline_run import PipelineRunsFilter
from dagster.core.storage.tags import TRIGGER_TAG
from dagster.utils import merge_dicts


class DauphinTriggerDefinition(dauphin.ObjectType):
    class Meta(object):
        name = "TriggerDefinition"

    name = dauphin.NonNull(dauphin.String)
    pipeline_name = dauphin.NonNull(dauphin.String)
    solid_selection = dauphin.List(dauphin.String)
    mode = dauphin.NonNull(dauphin.String)
    runConfigOrError = dauphin.Field("RunConfigOrError")
    tagsOrError = dauphin.NonNull("TagsListOrError")
    runs = dauphin.Field(
        dauphin.non_null_list("PipelineRun"),
        filter=dauphin.Argument("PipelineRunsFilter"),
        cursor=dauphin.String(),
        limit=dauphin.Int(),
    )

    def resolve_runConfigOrError(self, graphene_info):
        return get_trigger_execution_config(graphene_info, self._external_triggered_execution)

    def resolve_tagsOrError(self, graphene_info):
        return get_trigger_execution_tags(graphene_info, self._external_triggered_execution)

    def resolve_runs(self, graphene_info, **kwargs):
        filters = kwargs.get("filter")
        trigger_tags = {
            TRIGGER_TAG: self._external_triggered_execution.name,
        }
        if filters is not None:
            filters = filters.to_selector()
            runs_filter = PipelineRunsFilter(
                run_ids=filters.run_ids,
                pipeline_name=filters.pipeline_name,
                status=filters.status,
                tags=merge_dicts(filters.tags, trigger_tags),
            )
        else:
            runs_filter = PipelineRunsFilter(tags=trigger_tags)

        return get_runs(
            graphene_info, runs_filter, cursor=kwargs.get("cursor"), limit=kwargs.get("limit")
        )

    def __init__(self, external_triggered_execution):
        self._external_triggered_execution = check.inst_param(
            external_triggered_execution, "external_triggered_execution", ExternalTriggeredExecution
        )

        super(DauphinTriggerDefinition, self).__init__(
            name=external_triggered_execution.name,
            pipeline_name=external_triggered_execution.pipeline_name,
            solid_selection=external_triggered_execution.solid_selection,
            mode=external_triggered_execution.mode,
        )


class DauphinTriggerDefinitions(dauphin.ObjectType):
    class Meta(object):
        name = "TriggerDefinitions"

    results = dauphin.non_null_list("TriggerDefinition")


class DauphinTriggerDefinitionsOrError(dauphin.Union):
    class Meta(object):
        name = "TriggerDefinitionsOrError"
        types = (DauphinTriggerDefinitions, DauphinRepositoryNotFoundError, DauphinPythonError)


class DauphinTriggerDefinitionOrError(dauphin.Union):
    class Meta(object):
        name = "TriggerDefinitionOrError"
        types = (
            DauphinTriggerDefinition,
            DauphinRepositoryNotFoundError,
            DauphinTriggerDefinitionNotFoundError,
            DauphinPythonError,
        )
