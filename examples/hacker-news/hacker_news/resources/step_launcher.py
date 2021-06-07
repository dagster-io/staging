import io
import os
import pickle
import re
import tempfile
import time
from dataclasses import dataclass
from typing import Optional

from dagster import Field, resource
from dagster.core.definitions.step_launcher import StepLauncher
from dagster.core.errors import raise_execution_interrupts
from dagster.core.events import log_step_event
from dagster.core.execution.context.init import InitResourceContext
from dagster.core.execution.context.system import StepExecutionContext
from dagster.core.execution.plan.external_step import (
    PICKLED_EVENTS_FILE_NAME,
    PICKLED_STEP_RUN_REF_FILE_NAME,
    step_context_to_step_run_ref,
)
from dagster_gcp.dataproc.types import DataprocError
from google.cloud import dataproc_v1 as dataproc  # type: ignore[attr-defined]
from google.cloud import storage  # type: ignore[attr-defined]
from hacker_news.resources import dataproc_step_main

CODE_ZIP_NAME = "code.zip"


@resource(
    config_schema={
        "project_id": str,
        "region": str,
        "deploy_local_pipeline_package": Field(
            bool,
            default_value=False,
            is_required=False,
            description="If set, before every step run, the launcher will zip up all the code in "
            "local_pipeline_package_path, upload it to GCS, and pass it to spark-submit's "
            "--py-files option. This gives the remote process access to up-to-date user code. "
            "If not set, the assumption is that some other mechanism is used for distributing code "
            "to the Dataproc cluster. If this option is set to True, gcs_pipeline_package_path "
            "should not also be set.",
        ),
    },
    required_resource_keys={"dataproc_cluster_client", "dataproc_job_client", "gcs_client"},
)
def dataproc_step_launcher(init_context: InitResourceContext):
    return DataprocStepLauncher(init_context)


@dataclass
class DataprocStepLauncher(StepLauncher):
    init_context: InitResourceContext
    local_pipeline_package_path: Optional[str] = "."
    staging_prefix: Optional[str] = "dagster-steplauncher"
    staging_bucket: Optional[str] = "dagster-scratch-ccdfe1e"
    deploy_local_pipeline_package: bool = True

    def create_cluster(self, step_context: StepExecutionContext):
        """
        Launches the Dataproc cluster for each step execution.
        """
        project_id = self.init_context.resource_config["project_id"]
        region = self.init_context.resource_config["region"]
        cluster_name = f"{step_context.step.key}-{step_context.pipeline_run.run_id}"
        cluster = {
            "project_id": project_id,
            "cluster_name": cluster_name,
            "config": {
                "gce_cluster_config": {"metadata": {"PIP_PACKAGES": "dagster dagster_gcp"}},
                "software_config": {"image_version": "2.0.2-debian10"},
                "initialization_actions": [
                    {
                        "executable_file": f"gs://goog-dataproc-initialization-actions-{region}/python/pip-install.sh"
                    }
                ],
            },
        }
        try:
            request = dataproc.CreateClusterRequest(
                project_id=project_id,
                region=region,
                cluster=cluster,
            )

            cc = self.init_context.resources.dataproc_cluster_client
            op: dataproc.ClusterOperationMetadata = cc.create_cluster(request=request)
            result = op.result()
            step_context.log.debug(f"Cluster created successfully: {result.cluster_name}")

        except Exception as e:
            raise DataprocError(e)

    def _post_artifacts(self, log, step_run_ref, run_id, step_key):
        """
        Synchronize the step run ref and pyspark code to a GCS staging bucket for use with
        Cloud Dataproc.

        For the zip file, consider the following toy example:

            # Folder: my_pyspark_project/
            # a.py
            def foo():
                print(1)

            # b.py
            def bar():
                print(2)

            # main.py
            from a import foo
            from b import bar

            foo()
            bar()

        This will zip up `my_pyspark_project/` as `my_pyspark_project.zip`. Then, when running
        `spark-submit --py-files my_pyspark_project.zip emr_step_main.py` on Dataproc this will
        print 1, 2.
        """
        from dagster_pyspark.utils import build_pyspark_zip

        with tempfile.TemporaryDirectory() as temp_dir:
            # Upload step run ref
            def _upload_file_to_gcs(local_path, gcs_filename):
                key = self._artifact_gcs_key(run_id, step_key, gcs_filename)
                gcs_uri = self._artifact_gcs_uri(run_id, step_key, gcs_filename)
                log.debug(f"Uploading file {local_path} to {gcs_uri}")

                bucket = self.init_context.resources.gcs_client.get_bucket(self.staging_bucket)
                bucket.blob(key).upload_from_filename(local_path)

            _upload_file_to_gcs(dataproc_step_main.__file__, self._main_file_name())

            if self.deploy_local_pipeline_package:
                # Zip and upload package containing pipeline
                zip_local_path = os.path.join(temp_dir, CODE_ZIP_NAME)

                build_pyspark_zip(zip_local_path, self.local_pipeline_package_path)
                _upload_file_to_gcs(zip_local_path, CODE_ZIP_NAME)

            # Create step run ref pickle file
            step_run_ref_local_path = os.path.join(temp_dir, PICKLED_STEP_RUN_REF_FILE_NAME)
            with open(step_run_ref_local_path, "wb") as step_pickle_file:
                pickle.dump(step_run_ref, step_pickle_file)

            _upload_file_to_gcs(step_run_ref_local_path, PICKLED_STEP_RUN_REF_FILE_NAME)

    def _artifact_gcs_key(self, run_id, step_key, filename):
        return "/".join([self.staging_prefix, run_id, step_key, os.path.basename(filename)])

    def _artifact_gcs_uri(self, run_id, step_key, filename):
        key = self._artifact_gcs_key(run_id, step_key, filename)
        return f"gs://{self.staging_bucket}/{key}"

    def _main_file_name(self) -> str:
        return os.path.basename(dataproc_step_main.__file__)

    def _job_submit(self, step_context, run_id: str) -> dataproc.Job:
        step_key = step_context.step.key
        cluster_name = f"{step_key}-{step_context.pipeline_run.run_id}"

        job = {
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {
                "main_python_file_uri": self._artifact_gcs_uri(
                    run_id, step_key, self._main_file_name()
                ),
                "python_file_uris": [
                    self._artifact_gcs_uri(run_id, step_key, CODE_ZIP_NAME),
                    self._artifact_gcs_uri(run_id, step_key, PICKLED_STEP_RUN_REF_FILE_NAME),
                ],
                "args": [
                    self.init_context.resource_config["project_id"],
                    self.staging_bucket,
                    self._artifact_gcs_key(run_id, step_key, PICKLED_STEP_RUN_REF_FILE_NAME),
                ],
            },
        }
        return self.init_context.resources.dataproc_job_client.submit_job(
            request={
                "project_id": self.init_context.resource_config["project_id"],
                "region": self.init_context.resource_config["region"],
                "job": job,
            }
        )

    def execute_remote_step(self, step_context: StepExecutionContext, prior_attempts_count: int):
        """
        Execute the pyspark code on the remote cluster, and yield events back to the execution.
        """
        # get pointer to local repo
        step_run_ref = step_context_to_step_run_ref(
            step_context, prior_attempts_count, self.local_pipeline_package_path
        )

        run_id = step_context.pipeline_run.run_id
        log = step_context.log

        step_key = step_run_ref.step_key
        # send to gcs
        self._post_artifacts(log, step_run_ref, run_id, step_key)
        # submit job
        job = self._job_submit(step_context, run_id)
        for event in self.wait_for_completion(job, run_id, step_key):
            log_step_event(step_context, event)
            yield event

    def is_dataproc_step_complete(self, state):
        return state in [
            dataproc.JobStatus.State.DONE,
            dataproc.JobStatus.State.CANCELLED,
            dataproc.JobStatus.State.ERROR,
            dataproc.JobStatus.State.ATTEMPT_FAILURE,
        ]

    def handle_output(self, curr_job):  # TODO: do something more extensive with this output
        matches = re.match("gs://(.*?)/(.*)", curr_job.driver_output_resource_uri)

        _output = (
            self.init_context.resources.gcs_client.get_bucket(matches.group(1))
            .blob(f"{matches.group(2)}.000000000")
            .download_as_string()
        )

    def wait_for_completion(
        self,
        job: dataproc.Job,
        run_id: str,
        step_key: str,
        check_interval: int = 5,
    ):
        project_id = self.init_context.resource_config["project_id"]
        region = self.init_context.resource_config["region"]

        done = False
        all_events: list = []
        # If this is being called within a `capture_interrupts` context, allow interrupts
        # while waiting for the pyspark execution to complete, so that we can terminate slow or
        # hanging steps
        while not done:
            with raise_execution_interrupts():
                time.sleep(check_interval)

                # get refreshed job object from Dataproc API
                curr_job: dataproc.Job = self.init_context.resources.dataproc_job_client.get_job(
                    project_id=project_id, region=region, job_id=job.reference.job_id
                )

                state: dataproc.JobStatus.State = curr_job.status.state

                # check if state is one of the terminal job states
                done = self.is_dataproc_step_complete(state)

                all_events_new = self.read_events(project_id, run_id, step_key)

            if len(all_events_new) > len(all_events):
                for i in range(len(all_events), len(all_events_new)):
                    yield all_events_new[i]
                all_events = all_events_new

        # Dataproc job output gets saved to the Google Cloud Storage bucket
        # allocated to the job. Use a regex to obtain the bucket and blob info.
        self.handle_output(curr_job)

    def read_events(self, project_id: str, run_id: str, step_key: str):
        client = storage.client.Client(project=project_id)
        bucket = client.get_bucket(self.staging_bucket)
        blob = bucket.blob(self._artifact_gcs_key(run_id, step_key, PICKLED_EVENTS_FILE_NAME))

        # The file might not be there yet, which is fine
        if not blob.exists():
            return []

        buffer = io.BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)
        return pickle.loads(buffer.read())

    def delete_cluster(self, step_context: StepExecutionContext):
        """
        Destroy the Dataproc cluster.
        """
        project_id = self.init_context.resource_config["project_id"]
        region = self.init_context.resource_config["region"]
        cluster_name = f"{step_context.step.key}-{step_context.pipeline_run.run_id}"

        request = dataproc.DeleteClusterRequest(
            project_id=project_id,
            region=region,
            cluster_name=cluster_name,
        )

        cc = self.init_context.resources.dataproc_cluster_client
        op: dataproc.ClusterOperationMetadata = cc.delete_cluster(request)
        result = op.result()

        step_context.log.debug(f"Cluster destroyed successfully: {result.cluster_name}")

    def launch_step(self, step_context: StepExecutionContext, prior_attempts_count: int):
        self.create_cluster(step_context)
        for event in self.execute_remote_step(step_context, prior_attempts_count):
            yield event
        self.delete_cluster(step_context)
