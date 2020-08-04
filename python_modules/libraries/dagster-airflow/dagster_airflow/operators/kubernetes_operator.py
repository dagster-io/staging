import sys
import time

from airflow.contrib.kubernetes import kube_client, pod_generator, pod_launcher
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from dagster_airflow.vendor.kubernetes_pod_operator import KubernetesPodOperator

from dagster import __version__ as dagster_version
from dagster import check
from dagster.cli.api import ExecuteStepArgs
from dagster.cli.api_utils import filter_dagster_events_from_logs
from dagster.core.events import EngineEventData
from dagster.core.instance import AIRFLOW_EXECUTION_DATE_STR, DagsterInstance
from dagster.serdes import serialize_dagster_namedtuple
from dagster.utils.error import serializable_error_info_from_exc_info

from .util import check_events_for_failures, check_events_for_skips, get_aws_environment

# For retries on log retrieval
LOG_RETRIEVAL_MAX_ATTEMPTS = 5
LOG_RETRIEVAL_WAITS_BETWEEN_ATTEMPTS_SEC = 5


class DagsterKubernetesPodOperator(KubernetesPodOperator):
    '''Dagster operator for Apache Airflow.

    Wraps a modified KubernetesPodOperator.
    '''

    # py2 compat
    # pylint: disable=keyword-arg-before-vararg
    def __init__(self, operator_parameters, *args):
        kwargs = operator_parameters.op_kwargs
        self.pipeline_name = operator_parameters.pipeline_name
        self.pipeline_snapshot = operator_parameters.pipeline_snapshot
        self.execution_plan_snapshot = operator_parameters.execution_plan_snapshot
        self.parent_pipeline_snapshot = operator_parameters.parent_pipeline_snapshot
        kwargs['name'] = 'dagster.{pipeline_name}.{task_id}'.format(
            pipeline_name=self.pipeline_name, task_id=operator_parameters.task_id
        ).replace(
            '_', '-'  # underscores are not permissible DNS names
        )

        self.run_config = operator_parameters.run_config
        self.mode = operator_parameters.mode
        self.step_keys = operator_parameters.step_keys
        self.recon_repo = operator_parameters.recon_repo
        self._run_id = None
        # self.instance might be None in, for instance, a unit test setting where the operator
        # was being directly instantiated without passing through make_airflow_dag
        self.instance = (
            DagsterInstance.from_ref(operator_parameters.instance_ref)
            if operator_parameters.instance_ref
            else None
        )

        # Add AWS creds
        self.env_vars = kwargs.get('env_vars', {})
        for k, v in get_aws_environment().items():
            self.env_vars.setdefault(k, v)

        kwargs.setdefault('labels', {})
        kwargs['labels'].setdefault('dagster_pipeline', self.pipeline_name)
        kwargs['labels'].setdefault('app.kubernetes.io/name', 'dagster')
        kwargs['labels'].setdefault('app.kubernetes.io/instance', self.pipeline_name)
        kwargs['labels'].setdefault('app.kubernetes.io/version', dagster_version)
        kwargs['labels'].setdefault('app.kubernetes.io/component', 'pipeline-execution')
        kwargs['labels'].setdefault('app.kubernetes.io/part-of', 'dagster-airflow')
        kwargs['labels'].setdefault('app.kubernetes.io/managed-by', 'dagster-airflow')

        # The xcom mechanism for the pod operator is very unlike that of the Docker operator, so
        # we disable it
        if 'xcom_push' in kwargs:
            self.log.warning(
                'xcom_push cannot be enabled with the DagsterKubernetesPodOperator, disabling'
            )
        kwargs['xcom_push'] = False

        super(DagsterKubernetesPodOperator, self).__init__(
            task_id=operator_parameters.task_id, dag=operator_parameters.dag, *args, **kwargs
        )

    @property
    def run_id(self):
        return getattr(self, '_run_id', '')

    def query(self, airflow_ts):
        check.opt_str_param(airflow_ts, 'airflow_ts')

        pipeline_origin = self.recon_repo.get_reconstructable_pipeline(
            self.pipeline_name
        ).get_origin()

        args = ExecuteStepArgs(
            pipeline_origin=pipeline_origin,
            pipeline_run_id=self.run_id,
            instance_ref=self.instance.get_ref() if self.instance else None,
            mode=self.mode,
            step_keys_to_execute=self.step_keys,
            run_config=self.run_config,
            retries_dict=None,
        )

        query = [
            'dagster',
            'api',
            'execute_step_with_structured_logs',
            '{}'.format(serialize_dagster_namedtuple(args)),
        ]
        self.log.info('Executing query: {query}'.format(query=query))

        return query

    def execute(self, context):

        if 'run_id' in self.params:
            self._run_id = self.params['run_id']
        elif 'dag_run' in context and context['dag_run'] is not None:
            self._run_id = context['dag_run'].run_id

        # return to original execute code:
        try:
            client = kube_client.get_kube_client(
                in_cluster=self.in_cluster,
                cluster_context=self.cluster_context,
                config_file=self.config_file,
            )
            gen = pod_generator.PodGenerator()

            for mount in self.volume_mounts:
                gen.add_mount(mount)
            for volume in self.volumes:
                gen.add_volume(volume)

            pod = gen.make_pod(
                namespace=self.namespace,
                image=self.image,
                pod_id=self.name,
                cmds=self.cmds,
                arguments=self.query(context.get('ts')),
                labels=self.labels,
            )

            pod.service_account_name = self.service_account_name
            pod.secrets = self.secrets
            pod.envs = self.env_vars
            pod.image_pull_policy = self.image_pull_policy
            pod.image_pull_secrets = self.image_pull_secrets
            pod.annotations = self.annotations
            pod.resources = self.resources
            pod.affinity = self.affinity
            pod.node_selectors = self.node_selectors
            pod.hostnetwork = self.hostnetwork
            pod.tolerations = self.tolerations
            pod.configmaps = self.configmaps
            pod.security_context = self.security_context

            launcher = pod_launcher.PodLauncher(kube_client=client, extract_xcom=self.xcom_push)
            try:
                if self.instance:
                    tags = (
                        {AIRFLOW_EXECUTION_DATE_STR: context.get('ts')} if 'ts' in context else {}
                    )

                    run = self.instance.register_managed_run(
                        pipeline_name=self.pipeline_name,
                        run_id=self.run_id,
                        run_config=self.run_config,
                        mode=self.mode,
                        solids_to_execute=None,
                        step_keys_to_execute=None,
                        tags=tags,
                        root_run_id=None,
                        parent_run_id=None,
                        pipeline_snapshot=self.pipeline_snapshot,
                        execution_plan_snapshot=self.execution_plan_snapshot,
                        parent_pipeline_snapshot=self.parent_pipeline_snapshot,
                    )

                # we won't use the "result", which is the pod's xcom json file
                (final_state, _) = launcher.run_pod(
                    pod, startup_timeout=self.startup_timeout_seconds, get_logs=self.get_logs
                )

                # fetch the last line independently of whether logs were read
                # unbelievably, if you set tail_lines=1, the returned json has its double quotes
                # turned into unparseable single quotes
                events = None
                num_attempts = 0
                while not events and num_attempts < LOG_RETRIEVAL_MAX_ATTEMPTS:
                    raw_res = client.read_namespaced_pod_log(
                        name=pod.name, namespace=pod.namespace, container='base'
                    )

                    events = filter_dagster_events_from_logs(raw_res.split('\n'))

                    time.sleep(LOG_RETRIEVAL_WAITS_BETWEEN_ATTEMPTS_SEC)
                    num_attempts += 1

                if not events:
                    msg = 'No dagster events found in pod logs'
                    self.instance.report_engine_event(
                        msg,
                        run,
                        EngineEventData.engine_error(
                            serializable_error_info_from_exc_info(sys.exc_info())
                        ),
                        self.__class__,
                    )
                    raise AirflowException(msg)

                if self.instance:
                    for event in events:
                        self.instance.handle_new_event(event)

                dagster_events = [e.dagster_event for e in events]
                check_events_for_failures(dagster_events)
                check_events_for_skips(dagster_events)
                return dagster_events

            finally:
                self._run_id = None

                if self.is_delete_operator_pod:
                    launcher.delete_pod(pod)

            if final_state != State.SUCCESS:
                raise AirflowException('Pod returned a failure: {state}'.format(state=final_state))
            # note the lack of returning the default xcom
        except AirflowException as ex:
            raise AirflowException('Pod Launching failed: {error}'.format(error=ex))
