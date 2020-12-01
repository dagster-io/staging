import json
import os
import weakref

import docker
from dagster import Field, StringSource, check
from dagster.core.host_representation import ExternalPipeline
from dagster.core.instance import DagsterInstance
from dagster.core.launcher.base import RunLauncher
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.grpc.types import ExecuteRunArgs
from dagster.serdes import ConfigurableClass, serialize_dagster_namedtuple


class DockerRunLauncher(RunLauncher, ConfigurableClass):
    """Launches runs in a Docker container.
    """

    def __init__(self, inst_data=None, image=None, registry=None, env_vars=None, network=None):
        self._instance_weakref = None
        self._inst_data = inst_data
        self._image = image
        self._registry = registry
        self._env_vars = env_vars
        self._network = network

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "image": Field(
                StringSource,
                is_required=False,
                description="The docker image to be used if the repository does not specify one.",
            ),
            "registry": Field(
                {
                    "url": Field(StringSource),
                    "username": Field(StringSource),
                    "password": Field(StringSource),
                },
                is_required=False,
                description="Information for using a non local/public docker registry",
            ),
            "env_vars": Field(
                [str],
                is_required=False,
                description="The list of environment variables names to forward in to the docker container",
            ),
            "network": Field(
                str,
                is_required=False,
                description="Name of the network this container to which to connect the launched container at creation time",
            ),
        }

    @staticmethod
    def from_config_value(inst_data, config_value):
        return DockerRunLauncher(inst_data=inst_data, **config_value)

    @property
    def _instance(self):
        return self._instance_weakref() if self._instance_weakref else None

    def initialize(self, instance):
        check.inst_param(instance, "instance", DagsterInstance)
        check.invariant(self._instance is None, "Must only call initialize once")
        # Store a weakref to avoid a circular reference / enable GC
        self._instance_weakref = weakref.ref(instance)

    def launch_run(self, instance, run, external_pipeline):
        """Subclasses must implement this method."""

        check.inst_param(run, "run", PipelineRun)
        check.inst_param(external_pipeline, "external_pipeline", ExternalPipeline)

        docker_image = external_pipeline.get_python_origin().repository_origin.container_image

        if not docker_image:
            docker_image = self._image

        if not docker_image:
            raise Exception("No docker image specified by the repository")

        client = docker.client.from_env()

        if self._registry:
            client.login(
                registry=self._registry["url"],
                username=self._registry["username"],
                password=self._registry["password"],
            )

        input_json = serialize_dagster_namedtuple(
            ExecuteRunArgs(
                pipeline_origin=external_pipeline.get_python_origin(),
                pipeline_run_id=run.run_id,
                instance_ref=instance.get_ref(),
            )
        )

        command = "dagster api execute_run_with_structured_logs {}".format(json.dumps(input_json))

        docker_env = (
            {env_name: os.getenv(env_name) for env_name in self._env_vars} if self._env_vars else {}
        )

        client.containers.run(
            docker_image,
            command=command,
            detach=True,
            auto_remove=True,
            # pass through this worker's environment for things like AWS creds etc.
            environment=docker_env,
            network=self._network,
        )

        return run

    def can_terminate(self, run_id):
        check.str_param(run_id, "run_id")
        return False

    def terminate(self, run_id):
        check.str_param(run_id, "run_id")
        check.not_implemented("Termination not yet implemented")
