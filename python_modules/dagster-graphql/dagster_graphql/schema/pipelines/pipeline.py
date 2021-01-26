from functools import lru_cache

import graphene
import yaml
from dagster import check
from dagster.core.events.log import EventRecord
from dagster.core.host_representation.external import ExternalExecutionPlan, ExternalPipeline
from dagster.core.host_representation.external_data import ExternalPresetData
from dagster.core.host_representation.represented import RepresentedPipeline
from dagster.core.snap import ConfigSchemaSnapshot, LoggerDefSnap, ModeDefSnap, ResourceDefSnap
from dagster.core.storage.tags import TagType, get_tag_type

from ...implementation.fetch_assets import get_assets_for_run_id
from ...implementation.fetch_pipelines import get_pipeline_reference_or_raise
from ...implementation.fetch_runs import get_runs, get_stats, get_step_stats
from ...implementation.fetch_schedules import get_schedules_for_pipeline
from ...implementation.fetch_sensors import get_sensors_for_pipeline
from ...implementation.utils import UserFacingGraphQLError, capture_error
from ..asset_key import AssetKey
from ..config_types import ConfigTypeField
from ..dagster_types import DagsterType, DagsterTypeOrError, to_dagster_type
from ..errors import PipelineRunNotFoundError, PythonError
from ..execution import ExecutionPlan
from ..inputs import PipelineRunsFilter
from ..logs.compute_logs import ComputeLogs
from ..logs.events import PipelineRunStepStats, StepMaterializationEvent
from ..repository_origin import RepositoryOrigin
from ..runs import PipelineRun
from ..schedules.schedules import Schedule
from ..sensors import Sensor
from ..solids import Solid, SolidContainer, SolidHandle, build_solid_handles, build_solids
from ..tags import PipelineTag
from ..util import non_null_list
from .mode import Mode
from .pipeline_ref import PipelineReference
from .pipeline_run_stats import PipelineRunStatsOrError
from .status import PipelineRunStatus


class AssetMaterialization(graphene.ObjectType):
    def __init__(self, event):
        self._event = check.inst_param(event, "event", EventRecord)
        check.invariant(
            isinstance(event.dagster_event.step_materialization_data, StepMaterializationData)
        )

    materializationEvent = graphene.NonNull(StepMaterializationEvent)
    runOrError = graphene.NonNull(lambda: PipelineRunOrError)
    partition = graphene.Field(graphene.String)

    def resolve_materializationEvent(self, graphene_info):
        from ..runs import construct_basic_params

        return StepMaterializationEvent(
            materialization=self._event.dagster_event.step_materialization_data.materialization,
            **construct_basic_params(self._event),
        )

    def resolve_runOrError(self, graphene_info):
        from ...implementation.fetch_runs import get_run_by_id

        return get_run_by_id(graphene_info, self._event.run_id)

    def resolve_partition(self, _graphene_info):
        return self._event.dagster_event.step_materialization_data.materialization.partition


class Asset(graphene.ObjectType):
    key = graphene.NonNull(AssetKey)
    assetMaterializations = graphene.Field(
        non_null_list(AssetMaterialization),
        partitions=graphene.List(graphene.String),
        cursor=graphene.String(),
        limit=graphene.Int(),
    )
    runs = graphene.Field(
        non_null_list(PipelineRun), cursor=graphene.String(), limit=graphene.Int(),
    )

    def resolve_assetMaterializations(self, graphene_info, **kwargs):
        from ...implementation.fetch_assets import get_asset_events

        return [
            AssetMaterialization(event=event)
            for event in get_asset_events(
                graphene_info,
                self.key,
                kwargs.get("partitions"),
                kwargs.get("cursor"),
                kwargs.get("limit"),
            )
        ]

    def resolve_runs(self, graphene_info, **kwargs):
        from ...implementation.fetch_assets import get_asset_run_ids

        cursor = kwargs.get("cursor")
        limit = kwargs.get("limit")

        run_ids = get_asset_run_ids(graphene_info, self.key)

        if not run_ids:
            return []

        # for now, handle cursor/limit here instead of in the DB layer
        if cursor:
            try:
                idx = run_ids.index(cursor)
                run_ids = run_ids[idx:]
            except ValueError:
                return []

        if limit:
            run_ids = run_ids[:limit]

        return [
            PipelineRun(r)
            for r in graphene_info.context.instance.get_runs(
                filters=PipelineRunsFilter(run_ids=run_ids)
            )
        ]


class PipelineRun(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    runId = graphene.NonNull(graphene.String)
    # Nullable because of historical runs
    pipelineSnapshotId = graphene.String()
    repositoryOrigin = graphene.Field(RepositoryOrigin)
    status = graphene.NonNull(PipelineRunStatus)
    pipeline = graphene.NonNull(PipelineReference)
    pipelineName = graphene.NonNull(graphene.String)
    solidSelection = graphene.List(graphene.NonNull(graphene.String))
    stats = graphene.NonNull(PipelineRunStatsOrError)
    stepStats = non_null_list(PipelineRunStepStats)
    computeLogs = graphene.Field(
        graphene.NonNull(ComputeLogs),
        stepKey=graphene.Argument(graphene.NonNull(graphene.String)),
        description="""
        Compute logs are the stdout/stderr logs for a given solid step computation
        """,
    )
    executionPlan = graphene.Field(ExecutionPlan)
    stepKeysToExecute = graphene.List(graphene.NonNull(graphene.String))
    runConfigYaml = graphene.NonNull(graphene.String)
    mode = graphene.NonNull(graphene.String)
    tags = non_null_list(PipelineTag)
    rootRunId = graphene.Field(graphene.String)
    parentRunId = graphene.Field(graphene.String)
    canTerminate = graphene.NonNull(graphene.Boolean)
    assets = non_null_list(Asset)

    def __init__(self, pipeline_run):
        super().__init__(
            runId=pipeline_run.run_id, status=pipeline_run.status, mode=pipeline_run.mode
        )
        self._pipeline_run = check.inst_param(pipeline_run, "pipeline_run", PipelineRun)

    def resolve_id(self, _):
        return self._pipeline_run.run_id

    def resolve_repositoryOrigin(self, graphene_info):
        return (
            RepositoryOrigin(self._pipeline_run.external_pipeline_origin.external_repository_origin)
            if self._pipeline_run.external_pipeline_origin
            else None
        )

    def resolve_pipeline(self, graphene_info):
        return get_pipeline_reference_or_raise(graphene_info, self._pipeline_run)

    def resolve_pipelineName(self, _graphene_info):
        return self._pipeline_run.pipeline_name

    def resolve_solidSelection(self, _graphene_info):
        return self._pipeline_run.solid_selection

    def resolve_pipelineSnapshotId(self, _):
        return self._pipeline_run.pipeline_snapshot_id

    def resolve_stats(self, graphene_info):
        return get_stats(graphene_info, self.run_id)

    def resolve_stepStats(self, graphene_info):
        return get_step_stats(graphene_info, self.run_id)

    def resolve_computeLogs(self, graphene_info, stepKey):
        return ComputeLogs(runId=self.run_id, stepKey=stepKey)

    def resolve_executionPlan(self, graphene_info):
        if not (
            self._pipeline_run.execution_plan_snapshot_id
            and self._pipeline_run.pipeline_snapshot_id
        ):
            return None

        instance = graphene_info.context.instance
        historical_pipeline = instance.get_historical_pipeline(
            self._pipeline_run.pipeline_snapshot_id
        )
        execution_plan_snapshot = instance.get_execution_plan_snapshot(
            self._pipeline_run.execution_plan_snapshot_id
        )
        return (
            ExecutionPlan(
                ExternalExecutionPlan(
                    execution_plan_snapshot=execution_plan_snapshot,
                    represented_pipeline=historical_pipeline,
                )
            )
            if execution_plan_snapshot and historical_pipeline
            else None
        )

    def resolve_stepKeysToExecute(self, _):
        return self._pipeline_run.step_keys_to_execute

    def resolve_runConfigYaml(self, _graphene_info):
        return yaml.dump(
            self._pipeline_run.run_config, default_flow_style=False, allow_unicode=True
        )

    def resolve_tags(self, graphene_info):
        return [
            PipelineTag(key=key, value=value)
            for key, value in self._pipeline_run.tags.items()
            if get_tag_type(key) != TagType.HIDDEN
        ]

    def resolve_rootRunId(self, _):
        return self._pipeline_run.root_run_id

    def resolve_parentRunId(self, _):
        return self._pipeline_run.parent_run_id

    @property
    def run_id(self):
        return self.runId

    def resolve_canTerminate(self, graphene_info):
        # short circuit if the pipeline run is in a terminal state
        if self._pipeline_run.is_finished:
            return False
        return graphene_info.context.instance.run_coordinator.can_cancel_run(self.run_id)

    def resolve_assets(self, graphene_info):
        return get_assets_for_run_id(graphene_info, self.run_id)


class IPipelineSnapshotMixin:
    # Mixin this class to implement IPipelineSnapshot
    #
    # Graphene has some strange properties that make it so that you cannot
    # implement ABCs nor use properties in an overridable way. So the way
    # the mixin works is that the target classes have to have a method
    # get_represented_pipeline()
    #

    def get_represented_pipeline(self):
        raise NotImplementedError()

    name = graphene.NonNull(graphene.String)
    description = graphene.String()
    id = graphene.NonNull(graphene.ID)
    pipeline_snapshot_id = graphene.NonNull(graphene.String)
    dagster_types = non_null_list(DagsterType)
    dagster_type_or_error = graphene.Field(
        graphene.NonNull(DagsterTypeOrError),
        dagsterTypeName=graphene.Argument(graphene.NonNull(graphene.String)),
    )
    solids = non_null_list(Solid)
    modes = non_null_list(Mode)
    solid_handles = graphene.Field(non_null_list(SolidHandle), parentHandleID=graphene.String())
    solid_handle = graphene.Field(
        SolidHandle, handleID=graphene.Argument(graphene.NonNull(graphene.String)),
    )
    tags = non_null_list(PipelineTag)
    runs = graphene.Field(
        non_null_list(PipelineRun), cursor=graphene.String(), limit=graphene.Int(),
    )
    schedules = non_null_list(Schedule)
    sensors = non_null_list(Sensor)
    parent_snapshot_id = graphene.String()

    def resolve_pipeline_snapshot_id(self, _):
        return self.get_represented_pipeline().identifying_pipeline_snapshot_id

    def resolve_id(self, _):
        return self.get_represented_pipeline().identifying_pipeline_snapshot_id

    def resolve_name(self, _):
        return self.get_represented_pipeline().name

    def resolve_description(self, _):
        return self.get_represented_pipeline().description

    def resolve_dagster_types(self, _graphene_info):
        represented_pipeline = self.get_represented_pipeline()
        return sorted(
            list(
                map(
                    lambda dt: to_dagster_type(represented_pipeline.pipeline_snapshot, dt.key),
                    [t for t in represented_pipeline.dagster_type_snaps if t.name],
                )
            ),
            key=lambda dagster_type: dagster_type.name,
        )

    @capture_error
    def resolve_dagster_type_or_error(self, _, **kwargs):
        type_name = kwargs["dagsterTypeName"]

        represented_pipeline = self.get_represented_pipeline()

        if not represented_pipeline.has_dagster_type_named(type_name):
            from .errors import DagsterTypeNotFoundError

            raise UserFacingGraphQLError(DagsterTypeNotFoundError(dagster_type_name=type_name))

        return to_dagster_type(
            represented_pipeline.pipeline_snapshot,
            represented_pipeline.get_dagster_type_by_name(type_name).key,
        )

    def resolve_solids(self, _graphene_info):
        represented_pipeline = self.get_represented_pipeline()
        return build_solids(represented_pipeline, represented_pipeline.dep_structure_index,)

    def resolve_modes(self, _):
        represented_pipeline = self.get_represented_pipeline()
        return [
            Mode(represented_pipeline.config_schema_snapshot, mode_def_snap)
            for mode_def_snap in sorted(
                represented_pipeline.mode_def_snaps, key=lambda item: item.name
            )
        ]

    def resolve_solid_handle(self, _graphene_info, handleID):
        return _get_solid_handles(self.get_represented_pipeline()).get(handleID)

    def resolve_solid_handles(self, _graphene_info, **kwargs):
        handles = _get_solid_handles(self.get_represented_pipeline())
        parentHandleID = kwargs.get("parentHandleID")

        if parentHandleID == "":
            handles = {key: handle for key, handle in handles.items() if not handle.parent}
        elif parentHandleID is not None:
            handles = {
                key: handle
                for key, handle in handles.items()
                if handle.parent and handle.parent.handleID.to_string() == parentHandleID
            }

        return [handles[key] for key in sorted(handles)]

    def resolve_tags(self, graphene_info):
        represented_pipeline = self.get_represented_pipeline()
        return [
            PipelineTag(key=key, value=value)
            for key, value in represented_pipeline.pipeline_snapshot.tags.items()
        ]

    def resolve_solidSelection(self, _graphene_info):
        return self.get_represented_pipeline().solid_selection

    def resolve_runs(self, graphene_info, **kwargs):
        runs_filter = PipelineRunsFilter(pipeline_name=self.get_represented_pipeline().name)
        return get_runs(graphene_info, runs_filter, kwargs.get("cursor"), kwargs.get("limit"))

    def resolve_schedules(self, graphene_info):
        represented_pipeline = self.get_represented_pipeline()
        if not isinstance(represented_pipeline, ExternalPipeline):
            # this is an historical pipeline snapshot, so there are not any associated running
            # schedules
            return []

        pipeline_selector = represented_pipeline.handle.to_selector()
        schedules = get_schedules_for_pipeline(graphene_info, pipeline_selector)
        return schedules

    def resolve_sensors(self, graphene_info):
        represented_pipeline = self.get_represented_pipeline()
        if not isinstance(represented_pipeline, ExternalPipeline):
            # this is an historical pipeline snapshot, so there are not any associated running
            # sensors
            return []

        pipeline_selector = represented_pipeline.handle.to_selector()
        sensors = get_sensors_for_pipeline(graphene_info, pipeline_selector)
        return sensors

    def resolve_parent_snapshot_id(self, _graphene_info):
        lineage_snapshot = self.get_represented_pipeline().pipeline_snapshot.lineage_snapshot
        if lineage_snapshot:
            return lineage_snapshot.parent_snapshot_id
        else:
            return None


class IPipelineSnapshot(graphene.Interface):
    name = graphene.NonNull(graphene.String)
    description = graphene.String()
    pipeline_snapshot_id = graphene.NonNull(graphene.String)
    dagster_types = non_null_list(DagsterType)
    dagster_type_or_error = graphene.Field(
        graphene.NonNull(DagsterTypeOrError),
        dagsterTypeName=graphene.Argument(graphene.NonNull(graphene.String)),
    )
    solids = non_null_list(Solid)
    modes = non_null_list(Mode)
    solid_handles = graphene.Field(non_null_list(SolidHandle), parentHandleID=graphene.String())
    solid_handle = graphene.Field(
        SolidHandle, handleID=graphene.Argument(graphene.NonNull(graphene.String)),
    )
    tags = non_null_list(PipelineTag)


class PipelinePreset(graphene.ObjectType):
    name = graphene.NonNull(graphene.String)
    solidSelection = graphene.List(graphene.NonNull(graphene.String))
    runConfigYaml = graphene.NonNull(graphene.String)
    mode = graphene.NonNull(graphene.String)
    tags = non_null_list(PipelineTag)

    def __init__(self, active_preset_data, pipeline_name):
        self._active_preset_data = check.inst_param(
            active_preset_data, "active_preset_data", ExternalPresetData
        )
        self._pipeline_name = check.str_param(pipeline_name, "pipeline_name")

    def resolve_name(self, _graphene_info):
        return self._active_preset_data.name

    def resolve_solidSelection(self, _graphene_info):
        return self._active_preset_data.solid_selection

    def resolve_runConfigYaml(self, _graphene_info):
        yaml_str = yaml.safe_dump(
            self._active_preset_data.run_config, default_flow_style=False, allow_unicode=True
        )
        return yaml_str if yaml_str else ""

    def resolve_mode(self, _graphene_info):
        return self._active_preset_data.mode

    def resolve_tags(self, graphene_info):
        return [
            PipelineTag(key=key, value=value)
            for key, value in self._active_preset_data.tags.items()
            if get_tag_type(key) != TagType.HIDDEN
        ]


class Pipeline(IPipelineSnapshotMixin, graphene.ObjectType):
    class Meta:
        interfaces = (SolidContainer, IPipelineSnapshot)

    id = graphene.NonNull(graphene.ID)
    presets = non_null_list(PipelinePreset)
    runs = graphene.Field(
        non_null_list(PipelineRun), cursor=graphene.String(), limit=graphene.Int(),
    )

    def __init__(self, external_pipeline):
        self._external_pipeline = check.inst_param(
            external_pipeline, "external_pipeline", ExternalPipeline
        )

    def resolve_id(self, _graphene_info):
        return self._external_pipeline.get_external_origin_id()

    def get_represented_pipeline(self):
        return self._external_pipeline

    def resolve_presets(self, _graphene_info):
        return [
            PipelinePreset(preset, self._external_pipeline.name)
            for preset in sorted(self._external_pipeline.active_presets, key=lambda item: item.name)
        ]


@lru_cache(maxsize=32)
def _get_solid_handles(represented_pipeline):
    check.inst_param(represented_pipeline, "represented_pipeline", RepresentedPipeline)
    return {
        str(item.handleID): item
        for item in build_solid_handles(
            represented_pipeline, represented_pipeline.dep_structure_index
        )
    }


class PipelineRunOrError(graphene.Union):
    class Meta:
        types = (PipelineRun, PipelineRunNotFoundError, PythonError)
