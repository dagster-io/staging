from dagster import DagsterInstance
from filter_pipeline_runs.filter_pipeline_runs.get_pipeline_runs import SUCCESS, get_pipeline_runs


def test_get_failed_runs(capsys):
    with DagsterInstance.local_temp() as instance:
        run_list = get_pipeline_runs(instance, [SUCCESS])
        captured = capsys.readouterr()

        assert run_list[0] in captured.err
