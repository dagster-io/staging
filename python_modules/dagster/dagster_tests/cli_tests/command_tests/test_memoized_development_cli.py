from dagster import seven
from dagster.cli.pipeline import execute_execute_command
from dagster.core.instance import DagsterInstance, InstanceType
from dagster.core.launcher import CliApiRunLauncher
from dagster.core.storage.event_log import ConsolidatedSqliteEventLogStorage
from dagster.core.storage.local_compute_log_manager import LocalComputeLogManager
from dagster.core.storage.root import LocalArtifactStorage
from dagster.core.storage.runs import SqliteRunStorage
from dagster.utils import file_relative_path


def test_execute_versioned_command():
    with seven.TemporaryDirectory() as temp_dir:
        run_store = SqliteRunStorage.from_local(temp_dir)
        event_store = ConsolidatedSqliteEventLogStorage(temp_dir)
        compute_log_manager = LocalComputeLogManager(temp_dir)
        instance = DagsterInstance(
            instance_type=InstanceType.PERSISTENT,
            local_artifact_storage=LocalArtifactStorage(temp_dir),
            run_storage=run_store,
            event_storage=event_store,
            compute_log_manager=compute_log_manager,
            run_launcher=CliApiRunLauncher(),
        )

        kwargs = {
            "config": (file_relative_path(__file__, "basic_pipeline_config_1.yaml"),),
            "pipeline": "basic_pipeline",
            "python_file": file_relative_path(__file__, "memoized_development_basic_pipelines.py"),
            "tags": '{"dagster/is_memoized_run": "true"}',
        }

        result = execute_execute_command(kwargs=kwargs, instance=instance)
        assert result.success

        kwargs["config"] = (file_relative_path(__file__, "basic_pipeline_config_2.yaml",),)

        result2 = execute_execute_command(kwargs=kwargs, instance=instance)
        assert result2.success
