import logging

import graphene
import yaml
from dagster import PipelineRun, check, seven
from dagster.core.execution.stats import StepEventStatus as DagsterStepEventStatus
from graphene.types.generic import GenericScalar

from ..implementation.fetch_runs import get_runs, get_runs_count
from .errors import (
    InvalidPipelineRunsFilterError,
    PipelineRunNotFoundError,
    PythonError,
    RunGroupNotFoundError,
)
from .tags import PipelineTag, TagType
from .util import non_null_list

StepEventStatus = graphene.Enum.from_enum(DagsterStepEventStatus)


class LaunchPipelineRunSuccess(graphene.ObjectType):
    run = graphene.Field(graphene.NonNull(PipelineRun))


class RunGroup(graphene.ObjectType):
    rootRunId = graphene.NonNull(graphene.String)
    runs = graphene.List(PipelineRun)

    def __init__(self, root_run_id, runs):
        check.str_param(root_run_id, "root_run_id")
        check.list_param(runs, "runs", PipelineRun)

        super().__init__(rootRunId=root_run_id, runs=runs)


class RunGroups(graphene.ObjectType):
    results = non_null_list(RunGroup)


launch_pipeline_run_result_types = (LaunchPipelineRunSuccess,)


class LaunchPipelineExecutionResult(graphene.Union):
    class Meta:
        from .backfill import pipeline_execution_error_types

        types = launch_pipeline_run_result_types + pipeline_execution_error_types


class LaunchPipelineReexecutionResult(graphene.Union):
    class Meta:
        from .backfill import pipeline_execution_error_types

        types = launch_pipeline_run_result_types + pipeline_execution_error_types


class PipelineRuns(graphene.ObjectType):
    def __init__(self, filters, cursor, limit):
        self._filters = filters
        self._cursor = cursor
        self._limit = limit

    results = non_null_list(PipelineRun)
    count = graphene.Int()

    def resolve_results(self, graphene_info):
        return get_runs(graphene_info, self._filters, self._cursor, self._limit)

    def resolve_count(self, graphene_info):
        return get_runs_count(graphene_info, self._filters)


class PipelineRunsOrError(graphene.Union):
    class Meta:
        types = (PipelineRuns, InvalidPipelineRunsFilterError, PythonError)


class RunGroupOrError(graphene.Union):
    class Meta:
        types = (RunGroup, RunGroupNotFoundError, PythonError)


class RunGroupsOrError(graphene.ObjectType):
    class Meta:
        types = (RunGroups, PythonError)

    results = non_null_list(RunGroup)


class RunConfigData(GenericScalar, graphene.Scalar):
    class Meta:
        description = """This type is used when passing in a configuration object
        for pipeline configuration. This is any-typed in the GraphQL type system,
        but must conform to the constraints of the dagster config type system"""
