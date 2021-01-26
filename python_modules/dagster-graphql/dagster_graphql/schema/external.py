import graphene
from dagster import check
from dagster.core.host_representation import (
    ExternalRepository,
    ExternalRepositoryOrigin,
    GrpcServerRepositoryLocationHandle,
    ManagedGrpcPythonEnvRepositoryLocationHandle,
)
from dagster.core.host_representation import RepositoryLocation as DagsterRepositoryLocation
from dagster.core.host_representation.grpc_server_state_subscriber import (
    LocationStateChangeEventType,
)
from dagster.utils.error import SerializableErrorInfo
from dagster_graphql.implementation.fetch_solids import get_solid, get_solids

from .errors import PythonError, RepositoryNotFoundError
from .partition_sets import PartitionSet
from .pipelines.pipeline import Pipeline
from .repository_origin import RepositoryOrigin
from .schedules import Schedule
from .sensors import Sensor
from .used_solid import UsedSolid
from .util import non_null_list

LocationStateChangeEventType = graphene.Enum.from_enum(LocationStateChangeEventType)


class RepositoryLocation(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    name = graphene.NonNull(graphene.String)
    is_reload_supported = graphene.NonNull(graphene.Boolean)
    environment_path = graphene.String()
    repositories = non_null_list(Repository)
    server_id = graphene.String()

    def __init__(self, location):
        self._location = check.inst_param(location, "location", RepositoryLocation)
        environment_path = (
            location.location_handle.executable_path
            if isinstance(location.location_handle, ManagedGrpcPythonEnvRepositoryLocationHandle)
            else None
        )

        server_id = (
            location.location_handle.server_id
            if isinstance(location.location_handle, GrpcServerRepositoryLocationHandle)
            else None
        )

        check.invariant(location.name is not None)

        super(RepositoryLocation, self).__init__(
            name=location.name,
            environment_path=environment_path,
            is_reload_supported=location.is_reload_supported,
            server_id=server_id,
        )

    def resolve_id(self, _):
        return self.name

    def resolve_repositories(self, _graphene_info):
        return [
            Repository(repository, self._location)
            for repository in self._location.get_repositories().values()
        ]


class Repository(graphene.ObjectType):
    def __init__(self, repository, repository_location):
        self._repository = check.inst_param(repository, "repository", ExternalRepository)
        self._repository_location = check.inst_param(
            repository_location, "repository_location", DagsterRepositoryLocation
        )
        super(Repository, self).__init__(name=repository.name)

    id = graphene.NonNull(graphene.ID)
    name = graphene.NonNull(graphene.String)
    location = graphene.NonNull(RepositoryLocation)
    pipelines = non_null_list(Pipeline)
    usedSolids = graphene.Field(non_null_list(UsedSolid))
    usedSolid = graphene.Field(UsedSolid, name=graphene.NonNull(graphene.String))
    origin = graphene.NonNull(RepositoryOrigin)
    partitionSets = non_null_list(PartitionSet)
    schedules = non_null_list(Schedule)
    sensors = non_null_list(Sensor)

    def resolve_id(self, _graphene_info):
        return self._repository.get_external_origin_id()

    def resolve_origin(self, _graphene_info):
        origin = self._repository.get_external_origin()
        return RepositoryOrigin(origin)

    def resolve_location(self, _graphene_info):
        return RepositoryLocation(self._repository_location)

    def resolve_schedules(self, graphene_info):

        schedules = self._repository.get_external_schedules()

        return sorted(
            [Schedule(graphene_info, schedule) for schedule in schedules],
            key=lambda schedule: schedule.name,
        )

    def resolve_sensors(self, graphene_info):
        sensors = self._repository.get_external_sensors()
        return sorted(
            [Sensor(graphene_info, sensor) for sensor in sensors], key=lambda sensor: sensor.name,
        )

    def resolve_pipelines(self, _graphene_info):
        return sorted(
            [Pipeline(pipeline) for pipeline in self._repository.get_all_external_pipelines()],
            key=lambda pipeline: pipeline.name,
        )

    def resolve_usedSolid(self, _graphene_info, name):
        return get_solid(self._repository, name)

    def resolve_usedSolids(self, _graphene_info):
        return get_solids(self._repository)

    def resolve_partitionSets(self, _graphene_info):
        return (
            PartitionSet(self._repository.handle, partition_set)
            for partition_set in self._repository.get_external_partition_sets()
        )


class RepositoryLocationLoadFailure(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    name = graphene.NonNull(graphene.String)
    error = graphene.NonNull(PythonError)

    def __init__(self, name, error):
        check.str_param(name, "name")
        check.inst_param(error, "error", SerializableErrorInfo)
        super(RepositoryLocationLoadFailure, self).__init__(name=name, error=PythonError(error))

    def resolve_id(self, _):
        return self.name


class RepositoryLocationOrLoadFailure(graphene.Union):
    class Meta:
        types = (RepositoryLocation, RepositoryLocationLoadFailure)


class RepositoryConnection(graphene.ObjectType):
    nodes = non_null_list(Repository)


class RepositoryLocationConnection(graphene.ObjectType):
    nodes = non_null_list(RepositoryLocationOrLoadFailure)


class LocationStateChangeEvent(graphene.ObjectType):
    event_type = graphene.NonNull(LocationStateChangeEventType)
    message = graphene.NonNull(graphene.String)
    location_name = graphene.NonNull(graphene.String)
    server_id = graphene.Field(graphene.String)


class LocationStateChangeSubscription(graphene.ObjectType):
    event = graphene.Field(graphene.NonNull(LocationStateChangeEvent))


def get_location_state_change_observable(graphene_info):
    context = graphene_info.context
    return context.location_state_events.map(
        lambda event: LocationStateChangeSubscription(
            event=LocationStateChangeEvent(
                event_type=event.event_type,
                location_name=event.location_name,
                message=event.message,
                server_id=event.server_id,
            ),
        )
    )


class RepositoriesOrError(graphene.Union):
    class Meta:
        types = (RepositoryConnection, PythonError)


class RepositoryLocationsOrError(graphene.Union):
    class Meta:
        types = (RepositoryLocationConnection, PythonError)


class RepositoryOrError(graphene.Union):
    class Meta:
        types = (PythonError, Repository, RepositoryNotFoundError)
