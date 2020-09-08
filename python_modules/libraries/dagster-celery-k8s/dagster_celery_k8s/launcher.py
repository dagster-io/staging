import weakref

import kubernetes
from dagster_celery.core_execution_loop import DELEGATE_MARKER
from dagster_celery.make_app import make_app
from dagster_celery.tags import DAGSTER_CELERY_QUEUE_TAG
from dagster_k8s.job import (
    DagsterK8sJobConfig,
    UserDefinedDagsterK8sConfig,
    construct_dagster_k8s_job,
    get_job_name_from_run_id,
    get_user_defined_k8s_config,
)
from dagster_k8s.utils import delete_job, wait_for_job_success

from dagster import DagsterInvariantViolationError, EventMetadataEntry, Field, Noneable, check
from dagster.config.field import resolve_to_config_type
from dagster.config.validate import process_config
from dagster.core.events import EngineEventData
from dagster.core.execution.retries import Retries
from dagster.core.host_representation import ExternalPipeline
from dagster.core.host_representation.handle import GrpcServerRepositoryLocationHandle
from dagster.core.instance import DagsterInstance
from dagster.core.launcher import RunLauncher
from dagster.core.origin import PipelineGrpcServerOrigin, PipelinePythonOrigin
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus
from dagster.serdes import (
    ConfigurableClass,
    ConfigurableClassData,
    pack_value,
    serialize_dagster_namedtuple,
    unpack_value,
)
from dagster.utils import frozentags, merge_dicts

from .config import CELERY_K8S_CONFIG_KEY, celery_k8s_config


class CeleryK8sRunLauncher(RunLauncher, ConfigurableClass):
    """In contrast to the :py:class:`K8sRunLauncher`, which launches pipeline runs as single K8s
    Jobs, this run launcher is intended for use in concert with
    :py:func:`dagster_celery_k8s.celery_k8s_job_executor`.

    With this run launcher, execution is delegated to:

        1. A run coordinator Kubernetes Job, which traverses the pipeline run execution plan and
           submits steps to Celery queues for execution;
        2. The step executions which are submitted to Celery queues are picked up by Celery workers,
           and each step execution spawns a step execution Kubernetes Job. See the implementation
           defined in :py:func:`dagster_celery_k8.executor.create_k8s_job_task`.

    You may configure a Dagster instance to use this RunLauncher by adding a section to your
    ``dagster.yaml`` like the following:

    .. code-block:: yaml

        run_launcher:
          module: dagster_k8s.launcher
          class: CeleryK8sRunLauncher
          config:
            instance_config_map: "dagster-k8s-instance-config-map"
            dagster_home: "/some/path"
            postgres_password_secret: "dagster-k8s-pg-password"
            broker: "some_celery_broker_url"
            backend: "some_celery_backend_url"

    As always when using a :py:class:`~dagster.serdes.ConfigurableClass`, the values
    under the ``config`` key of this YAML block will be passed to the constructor. The full list
    of acceptable values is given below by the constructor args.

    Args:
        instance_config_map (str): The ``name`` of an existing Volume to mount into the pod in
            order to provide a ConfigMap for the Dagster instance. This Volume should contain a
            ``dagster.yaml`` with appropriate values for run storage, event log storage, etc.
        dagster_home (str): The location of DAGSTER_HOME in the Job container; this is where the
            ``dagster.yaml`` file will be mounted from the instance ConfigMap specified above.
        postgres_password_secret (str): The name of the Kubernetes Secret where the postgres
            password can be retrieved. Will be mounted and supplied as an environment variable to
            the Job Pod.
        load_incluster_config (Optional[bool]):  Set this value if you are running the launcher
            within a k8s cluster. If ``True``, we assume the launcher is running within the target
            cluster and load config using ``kubernetes.config.load_incluster_config``. Otherwise,
            we will use the k8s config specified in ``kubeconfig_file`` (using
            ``kubernetes.config.load_kube_config``) or fall back to the default kubeconfig. Default:
            ``True``.
        kubeconfig_file (Optional[str]): The kubeconfig file from which to load config. Defaults to
            None (using the default kubeconfig).
        broker (Optional[str]): The URL of the Celery broker.
        backend (Optional[str]): The URL of the Celery backend.
        include (List[str]): List of includes for the Celery workers
        config_source: (Optional[dict]): Additional settings for the Celery app.
        retries: (Optional[dict]): Default retry configuration for Celery tasks.
        queued: (Optional[bool]): (Experimental) Set this value to launch runs via a Celery queue,
            enabling limits on concurrent runs
        default_queue: (Optional[str]): (Experimental) The Celery queue for run coordinator tasks.
            Can be overriden using `DAGSTER_CELERY_QUEUE_TAG` on the pipeline. If None, then the
            run coordinator won't be queued and will be sent directly to K8s. Defaults to
            `dagster-run-coordinators`.
    """

    def __init__(
        self,
        instance_config_map,
        dagster_home,
        postgres_password_secret,
        load_incluster_config=True,
        kubeconfig_file=None,
        broker=None,
        backend=None,
        include=None,
        config_source=None,
        retries=None,
        inst_data=None,
        queued=False,
        default_queue=None,
    ):
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)

        self.load_incluster_config = check.bool_param(
            load_incluster_config, 'load_incluster_config'
        )
        self.kubeconfig_file = check.opt_str_param(kubeconfig_file, "kubeconfig_file")
        if load_incluster_config:
            check.invariant(
                kubeconfig_file is None,
                "`kubeconfig_file` is set but `load_incluster_config` is True.",
            )
        else:
            check.opt_str_param(kubeconfig_file, "kubeconfig_file")

        self.instance_config_map = check.str_param(instance_config_map, "instance_config_map")
        self.dagster_home = check.str_param(dagster_home, "dagster_home")
        self.postgres_password_secret = check.str_param(
            postgres_password_secret, "postgres_password_secret"
        )
        self.broker = check.opt_str_param(broker, "broker")
        self.backend = check.opt_str_param(backend, "backend")
        self.include = check.opt_list_param(include, "include")
        self.config_source = check.opt_dict_param(config_source, "config_source")

        retries = check.opt_dict_param(retries, "retries") or {"enabled": {}}
        self.retries = Retries.from_config(retries)
        self._instance_ref = None

        self.queued = check.bool_param(queued, "queued")
        if not self.queued:
            check.invariant(
                default_queue is None, "`default_queue` is set but `queued` is not enabled"
            )
        self.default_queue = check.opt_str_param(
            default_queue, 'default_queue', default='dagster-run-coordinators'
        )

    @classmethod
    def config_type(cls):
        """Include all arguments required for DagsterK8sJobConfig along with additional arguments
        needed for the RunLauncher itself.
        """
        from dagster_celery.executor import CELERY_CONFIG

        job_cfg = DagsterK8sJobConfig.config_type_run_launcher()

        run_launcher_extra_cfg = {
            "load_incluster_config": Field(bool, is_required=False, default_value=True),
            "kubeconfig_file": Field(Noneable(str), is_required=False, default_value=None),
            "queued": Field(bool, is_required=False, default_value=False),
        }

        res = merge_dicts(job_cfg, run_launcher_extra_cfg)
        return merge_dicts(res, CELERY_CONFIG)

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    @property
    def inst_data(self):
        return self._inst_data

    @property
    def _instance(self):
        return self._instance_ref() if self._instance_ref else None

    def initialize(self, instance):
        check.inst_param(instance, "instance", DagsterInstance)
        # Store a weakref to avoid a circular reference / enable GC
        self._instance_ref = weakref.ref(instance)

    def launch_run(self, instance, run, external_pipeline):
        check.inst_param(instance, "instance", DagsterInstance)
        check.inst_param(run, "run", PipelineRun)
        check.inst_param(external_pipeline, "external_pipeline", ExternalPipeline)

        job_name = get_job_name_from_run_id(run.run_id)
        pod_name = job_name
        exc_config = _get_validated_celery_k8s_executor_config(run.run_config)

        job_image = None
        pipeline_origin = None
        env_vars = None
        if isinstance(external_pipeline.get_origin(), PipelineGrpcServerOrigin):
            if exc_config.get("job_image"):
                raise DagsterInvariantViolationError(
                    "Cannot specify job_image in executor config when loading pipeline "
                    "from GRPC server."
                )

            repository_location_handle = (
                external_pipeline.repository_handle.repository_location_handle
            )

            if not isinstance(repository_location_handle, GrpcServerRepositoryLocationHandle):
                raise DagsterInvariantViolationError(
                    "Expected RepositoryLocationHandle to be of type "
                    "GrpcServerRepositoryLocationHandle but found type {}".format(
                        type(repository_location_handle)
                    )
                )

            job_image = repository_location_handle.get_current_image()
            env_vars = {"DAGSTER_CURRENT_IMAGE": job_image}

            repository_name = external_pipeline.repository_handle.repository_name
            pipeline_origin = PipelinePythonOrigin(
                pipeline_name=external_pipeline.name,
                repository_origin=repository_location_handle.get_repository_python_origin(
                    repository_name
                ),
            )

        else:
            job_image = exc_config.get("job_image")
            if not job_image:
                raise DagsterInvariantViolationError(
                    "Cannot find job_image in celery-k8s executor config."
                )
            pipeline_origin = external_pipeline.get_origin()

        job_config = DagsterK8sJobConfig(
            dagster_home=self.dagster_home,
            instance_config_map=self.instance_config_map,
            postgres_password_secret=self.postgres_password_secret,
            job_image=check.str_param(job_image, "job_image"),
            image_pull_policy=exc_config.get("image_pull_policy"),
            image_pull_secrets=exc_config.get("image_pull_secrets"),
            service_account_name=exc_config.get("service_account_name"),
            env_config_maps=exc_config.get("env_config_maps"),
            env_secrets=exc_config.get("env_secrets"),
        )

        user_defined_k8s_config = get_user_defined_k8s_config(frozentags(external_pipeline.tags))

        from dagster.cli.api import ExecuteRunArgs

        input_json = serialize_dagster_namedtuple(
            # depends on DagsterInstance.get() returning the same instance
            # https://github.com/dagster-io/dagster/issues/2757
            ExecuteRunArgs(
                pipeline_origin=pipeline_origin, pipeline_run_id=run.run_id, instance_ref=None,
            )
        )

        job_namespace = exc_config.get("job_namespace")
        job_queue = run.tags.get(DAGSTER_CELERY_QUEUE_TAG, self.default_queue)

        if not self.queued or not job_queue:
            _launch_run_coordinator_job(
                instance,
                job_config,
                input_json,
                job_name,
                pod_name,
                user_defined_k8s_config,
                env_vars,
                job_namespace,
                run,
                self.load_incluster_config,
                self.kubeconfig_file,
                wait_for_job_to_succeed=False,
            )
        else:
            # Create celery task
            self._instance.report_engine_event(
                "Sending run coordinator task to Celery",
                run,
                EngineEventData([]),
                cls=CeleryK8sRunLauncher,
            )

            app = make_app(
                app_args={
                    "broker": self.broker,
                    "backend": self.backend,
                    "include": self.include,
                    "config_source": self.config_source,
                    "retries": self.retries,
                }
            )
            task = create_k8s_run_coordinator_job_task(app)
            task_signature = task.si(
                dagster_k8s_job_config_packed=pack_value(job_config),
                input_json=input_json,
                job_name=job_name,
                pod_name=pod_name,
                user_defined_k8s_config_packed=pack_value(user_defined_k8s_config),
                env_vars=env_vars,
                job_namespace=job_namespace,
                pipeline_run_packed=pack_value(run),
                load_incluster_config=self.load_incluster_config,
                kubeconfig_file=self.kubeconfig_file,
            )
            res = task_signature.apply_async(
                queue=job_queue,
                routing_key="{queue}.execute_run_coordinator_k8s_job".format(queue=job_queue),
            )
            res.forget()

        return run

    # https://github.com/dagster-io/dagster/issues/2741
    def can_terminate(self, run_id):
        check.str_param(run_id, "run_id")

        pipeline_run = self._instance.get_run_by_id(run_id)
        if not pipeline_run:
            return False

        if pipeline_run.status != PipelineRunStatus.STARTED:
            return False

        return True

    def terminate(self, run_id):
        check.str_param(run_id, "run_id")

        if not self.can_terminate(run_id):
            return False

        job_name = get_job_name_from_run_id(run_id)

        job_namespace = self.get_namespace_from_run_config(run_id)

        return delete_job(job_name=job_name, namespace=job_namespace)

    def get_namespace_from_run_config(self, run_id):
        check.str_param(run_id, "run_id")

        pipeline_run = self._instance.get_run_by_id(run_id)
        run_config = pipeline_run.run_config
        executor_config = _get_validated_celery_k8s_executor_config(run_config)
        return executor_config.get("job_namespace")


def _get_validated_celery_k8s_executor_config(run_config):
    check.dict_param(run_config, "run_config")

    check.invariant(
        CELERY_K8S_CONFIG_KEY in run_config.get("execution", {}),
        "{} execution must be configured in pipeline execution config to launch runs with "
        "CeleryK8sRunLauncher".format(CELERY_K8S_CONFIG_KEY),
    )

    execution_config_schema = resolve_to_config_type(celery_k8s_config())
    execution_run_config = run_config["execution"][CELERY_K8S_CONFIG_KEY].get("config", {})
    res = process_config(execution_config_schema, execution_run_config)

    check.invariant(
        res.success, "Incorrect {} execution schema provided".format(CELERY_K8S_CONFIG_KEY)
    )

    return res.value


def _launch_run_coordinator_job(
    instance,
    dagster_k8s_job_config,
    input_json,
    job_name,
    pod_name,
    user_defined_k8s_config,
    env_vars,
    job_namespace,
    pipeline_run,
    load_incluster_config,
    kubeconfig_file,
    wait_for_job_to_succeed=False,
):
    check.inst(instance, DagsterInstance)
    check.inst(dagster_k8s_job_config, DagsterK8sJobConfig)
    check.is_str(input_json)
    check.is_str(job_name)
    check.is_str(pod_name)
    check.inst(user_defined_k8s_config, UserDefinedDagsterK8sConfig)
    check.dict_param(env_vars, "env_vars")
    check.is_str(job_namespace)
    check.inst(pipeline_run, PipelineRun)
    check.bool_param(load_incluster_config, "load_incluster_config")
    check.opt_str_param(kubeconfig_file, "kubeconfig_file")
    check.bool_param(wait_for_job_to_succeed, 'wait_for_job_to_succeed')

    job = construct_dagster_k8s_job(
        dagster_k8s_job_config,
        command=["dagster"],
        args=["api", "execute_run_with_structured_logs", input_json],
        job_name=job_name,
        pod_name=pod_name,
        component="run_coordinator",
        user_defined_k8s_config=user_defined_k8s_config,
        env_vars=env_vars,
    )

    # For when launched via DinD or running the cluster
    if load_incluster_config:
        kubernetes.config.load_incluster_config()
    else:
        kubernetes.config.load_kube_config(kubeconfig_file)

    try:
        api = kubernetes.client.BatchV1Api()
        api.create_namespaced_job(body=job, namespace=job_namespace)
    except kubernetes.client.rest.ApiException as e:
        if e.reason == "Conflict":
            # There is an existing job with the same name so do not procede.
            instance.report_engine_event(
                "Did not create Kubernetes job {} for run coordinator since job name already "
                "exists, exiting.".format(job_name),
                pipeline_run,
                EngineEventData(
                    [
                        EventMetadataEntry.text(job_name, "Kubernetes Job name"),
                        EventMetadataEntry.text(pod_name, "Kubernetes Pod name"),
                        EventMetadataEntry.text(job_namespace, "Kubernetes Namespace"),
                    ],
                    marker_end=DELEGATE_MARKER,
                ),
                CeleryK8sRunLauncher,
            )
        else:
            instance.report_engine_event(
                "Encountered unexpected error while creating Kubernetes job {} for "
                "run coordinator, exiting.".format(job_name),
                pipeline_run,
                EngineEventData(
                    [
                        EventMetadataEntry.text(e, "Error"),
                        EventMetadataEntry.text(job_namespace, "Kubernetes Namespace"),
                    ]
                ),
                CeleryK8sRunLauncher,
            )
        return

    instance.report_engine_event(
        "Kubernetes run_coordinator job launched",
        pipeline_run,
        EngineEventData(
            [
                EventMetadataEntry.text(job_name, "Kubernetes Job name"),
                EventMetadataEntry.text(pod_name, "Kubernetes Pod name"),
                EventMetadataEntry.text(job_namespace, "Kubernetes Namespace"),
                EventMetadataEntry.text(pipeline_run.run_id, "Run ID"),
            ]
        ),
        cls=CeleryK8sRunLauncher,
    )

    if wait_for_job_to_succeed:
        wait_for_job_success(
            job_name=job_name,
            namespace=job_namespace,
            instance=instance,
            run_id=pipeline_run.run_id,
            step_job=False,
        )


def create_k8s_run_coordinator_job_task(celery_app, **task_kwargs):
    @celery_app.task(bind=True, name="execute_run_coordinator_k8s_job", **task_kwargs)
    def _execute_run_coordinator_k8s_job(
        _self,
        dagster_k8s_job_config_packed,
        input_json,
        job_name,
        pod_name,
        user_defined_k8s_config_packed,
        env_vars,
        job_namespace,
        pipeline_run_packed,
        load_incluster_config,
        kubeconfig_file,
    ):

        dagster_k8s_job_config = check.inst(
            unpack_value(
                check.dict_param(dagster_k8s_job_config_packed, "dagster_k8s_job_config_packed")
            ),
            DagsterK8sJobConfig,
        )
        check.is_str(input_json)
        check.is_str(job_name)
        check.is_str(pod_name)
        user_defined_k8s_config = check.inst(
            unpack_value(
                check.dict_param(user_defined_k8s_config_packed, "user_defined_k8s_config_packed")
            ),
            UserDefinedDagsterK8sConfig,
        )
        check.dict_param(env_vars, "env_vars")
        check.is_str(job_namespace)
        pipeline_run = check.inst(
            unpack_value(check.dict_param(pipeline_run_packed, "pipeline_run_packed")), PipelineRun,
        )
        check.bool_param(load_incluster_config, "load_incluster_config")
        check.opt_str_param(kubeconfig_file, "kubeconfig_file")

        instance = DagsterInstance.get()

        _launch_run_coordinator_job(
            instance,
            dagster_k8s_job_config,
            input_json,
            job_name,
            pod_name,
            user_defined_k8s_config,
            env_vars,
            job_namespace,
            pipeline_run,
            load_incluster_config,
            kubeconfig_file,
            wait_for_job_to_succeed=True,
        )

        instance.report_engine_event(
            "Run coordinator finished, Celery exiting",
            pipeline_run,
            EngineEventData([]),
            cls=CeleryK8sRunLauncher,
        )

    return _execute_run_coordinator_k8s_job
