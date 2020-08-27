import yaml
from graphql.execution.base import ResolveInfo

from dagster import check
from dagster.core.host_representation import RepositorySelector
from dagster.core.host_representation.external_data import ExternalExecutionParamsData

from .utils import capture_dauphin_error


@capture_dauphin_error
def get_trigger_definitions_or_error(graphene_info, repository_selector):
    check.inst_param(graphene_info, "graphene_info", ResolveInfo)
    check.inst_param(repository_selector, "repository_selector", RepositorySelector)

    location = graphene_info.context.get_repository_location(repository_selector.location_name)
    repository = location.get_repository(repository_selector.repository_name)
    external_triggers = repository.get_external_triggered_executions()

    results = [
        graphene_info.schema.type_named("TriggerDefinition")(external_trigger)
        for external_trigger in external_triggers
    ]

    return graphene_info.schema.type_named("TriggerDefinitions")(results=results)


@capture_dauphin_error
def get_trigger_definition_or_error(graphene_info, repository_selector, trigger_name):
    check.inst_param(graphene_info, "graphene_info", ResolveInfo)
    check.inst_param(repository_selector, "repository_selector", RepositorySelector)
    check.str_param(trigger_name, "trigger_name")

    location = graphene_info.context.get_repository_location(repository_selector.location_name)
    repository = location.get_repository(repository_selector.repository_name)
    external_triggers = repository.get_external_triggered_executions()

    matched = [
        external_trigger
        for external_trigger in external_triggers
        if external_trigger.name == trigger_name
    ]

    if not matched:
        return graphene_info.schema.type_named("TriggerDefinitionNotFoundError")(trigger_name)

    return graphene_info.schema.type_named("TriggerDefinition")(matched[0])


@capture_dauphin_error
def get_trigger_execution_config(graphene_info, external_triggered_execution):
    result = graphene_info.context.get_external_triggered_execution_param_data(
        external_triggered_execution.handle, external_triggered_execution.name
    )
    if isinstance(result, ExternalExecutionParamsData):
        run_config_yaml = yaml.safe_dump(result.run_config, default_flow_style=False)
        return graphene_info.schema.type_named("RunConfig")(
            yaml=run_config_yaml if run_config_yaml else ""
        )
    else:
        return graphene_info.schema.type_named("PythonError")(result.error)


@capture_dauphin_error
def get_trigger_execution_tags(graphene_info, external_triggered_execution):
    handle = external_triggered_execution.handle.repository_handle
    result = graphene_info.context.get_external_triggered_execution_param_data(
        handle, external_triggered_execution.name
    )
    if isinstance(result, ExternalExecutionParamsData):
        run_config_yaml = yaml.safe_dump(result.run_config, default_flow_style=False)
        return graphene_info.schema.type_named("RunConfig")(
            yaml=run_config_yaml if run_config_yaml else ""
        )
    else:
        return graphene_info.schema.type_named("PythonError")(result.error)
