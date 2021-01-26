from datetime import datetime

import graphene
from dagster import check
from dagster.core.host_representation import ExternalSensor, SensorSelector
from dagster.core.scheduler.job import JobState as DagsterJobState
from dagster.core.scheduler.job import JobStatus
from dagster.utils import datetime_as_float

from ..implementation.fetch_sensors import start_sensor, stop_sensor
from .errors import PythonError, RepositoryNotFoundError, SensorNotFoundError
from .jobs import FutureJobTick, JobState
from .util import non_null_list

SENSOR_DAEMON_INTERVAL = 30


class Sensor(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    jobOriginId = graphene.NonNull(graphene.String)
    name = graphene.NonNull(graphene.String)
    pipelineName = graphene.NonNull(graphene.String)
    solidSelection = graphene.List(graphene.String)
    mode = graphene.NonNull(graphene.String)
    sensorState = graphene.NonNull(JobState)
    nextTick = graphene.Field(FutureJobTick)

    def resolve_id(self, _):
        return f"{self.name}:{self.pipelineName}"

    def __init__(self, graphene_info, external_sensor):
        self._external_sensor = check.inst_param(external_sensor, "external_sensor", ExternalSensor)
        self._sensor_state = graphene_info.context.instance.get_job_state(
            self._external_sensor.get_external_origin_id()
        )

        if not self._sensor_state:
            # Also include a SensorState for a stopped sensor that may not
            # have a stored database row yet
            self._sensor_state = self._external_sensor.get_default_job_state()

        super(Sensor, self).__init__(
            name=external_sensor.name,
            jobOriginId=external_sensor.get_external_origin_id(),
            pipelineName=external_sensor.pipeline_name,
            solidSelection=external_sensor.solid_selection,
            mode=external_sensor.mode,
        )

    def resolve_sensorState(self, _graphene_info):
        return JobState(self._sensor_state)

    def resolve_nextTick(self, graphene_info):
        if self._sensor_state.status != JobStatus.RUNNING:
            return None

        latest_tick = graphene_info.context.instance.get_latest_job_tick(
            self._sensor_state.job_origin_id
        )
        if not latest_tick:
            return None

        next_timestamp = latest_tick.timestamp + SENSOR_DAEMON_INTERVAL
        if next_timestamp < datetime_as_float(datetime.now()):
            return None
        return FutureJobTick(next_timestamp)


class SensorOrError(graphene.Union):
    class Meta:
        types = (
            Sensor,
            SensorNotFoundError,
            PythonError,
        )


class Sensors(graphene.ObjectType):
    results = non_null_list(Sensor)


class SensorsOrError(graphene.Union):
    class Meta:
        types = (Sensors, RepositoryNotFoundError, PythonError)


class StartSensorMutation(graphene.Mutation):
    class Arguments:
        sensor_selector = graphene.NonNull(SensorSelector)

    Output = graphene.NonNull(SensorOrError)

    def mutate(self, graphene_info, sensor_selector):
        return start_sensor(graphene_info, SensorSelector.from_graphql_input(sensor_selector))


class StopSensorMutationResult(graphene.ObjectType):
    jobState = graphene.Field(JobState)

    def __init__(self, job_state):
        super().__init__()
        self._job_state = check.inst_param(job_state, "job_state", DagsterJobState)

    def resolve_jobState(self, _graphene_info):
        if not self._job_state:
            return None

        return JobState(job_state=self._job_state)


class StopSensorMutationResultOrError(graphene.Union):
    class Meta:
        types = (StopSensorMutationResult, PythonError)


class StopSensorMutation(graphene.Mutation):
    class Arguments:
        job_origin_id = graphene.NonNull(graphene.String)

    Output = graphene.NonNull(StopSensorMutationResultOrError)

    def mutate(self, graphene_info, job_origin_id):
        return stop_sensor(graphene_info, job_origin_id)
