from dagster import check
from dagster.core.host_representation import ExternalSensor
from dagster.core.storage.pipeline_run import PipelineRunsFilter
from dagster_graphql import dauphin


class DauphinSensorDefinition(dauphin.ObjectType):
    class Meta:
        name = "SensorDefinition"

    id = dauphin.NonNull(dauphin.ID)
    name = dauphin.NonNull(dauphin.String)
    pipeline_name = dauphin.NonNull(dauphin.String)
    solid_selection = dauphin.List(dauphin.String)
    mode = dauphin.NonNull(dauphin.String)

    status = dauphin.NonNull("JobStatus")
    runs = dauphin.Field(dauphin.non_null_list("PipelineRun"), limit=dauphin.Int())
    runs_count = dauphin.NonNull(dauphin.Int)
    ticks = dauphin.Field(dauphin.non_null_list("JobTick"), limit=dauphin.Int())

    def resolve_id(self, _):
        return "%s:%s" % (self.name, self.pipeline_name)

    def __init__(self, graphene_info, external_sensor):
        self._external_sensor = check.inst_param(external_sensor, "external_sensor", ExternalSensor)
        self._sensor_state = graphene_info.context.instance.get_job_state(
            self._external_sensor.get_external_origin_id()
        )

        if not self._sensor_state:
            # Also include a SensorState for a stopped sensor that may not
            # have a stored database row yet
            self._sensor_state = self._external_sensor.get_default_job_state()

        super(DauphinSensorDefinition, self).__init__(
            name=external_sensor.name,
            pipeline_name=external_sensor.pipeline_name,
            solid_selection=external_sensor.solid_selection,
            mode=external_sensor.mode,
        )

    def resolve_status(self, _graphene_info):
        return self._sensor_state.status

    def resolve_runs(self, graphene_info, **kwargs):
        return [
            graphene_info.schema.type_named("PipelineRun")(r)
            for r in graphene_info.context.instance.get_runs(
                filters=PipelineRunsFilter.for_sensor(self._external_sensor),
                limit=kwargs.get("limit"),
            )
        ]

    def resolve_runs_count(self, graphene_info):
        return graphene_info.context.instance.get_runs_count(
            filters=PipelineRunsFilter.for_sensor(self._external_sensor)
        )

    def resolve_ticks(self, graphene_info, limit=None):
        ticks = graphene_info.context.instance.get_job_ticks(
            self._external_sensor.get_external_origin_id()
        )

        if limit:
            ticks = ticks[:limit]

        return [graphene_info.schema.type_named("JobTick")(tick) for tick in ticks]
