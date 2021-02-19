from dagster import DagsterInstance
from dagster.core.storage.pipeline_run import PipelineRunStatus, PipelineRunsFilter


# start_run_marker_0
def get_pipeline_runs(instance):
    return instance.get_runs(PipelineRunsFilter(statuses=[PipelineRunStatus.FAILURE]))


if __name__ == "__main__":
    get_pipeline_runs(DagsterInstance.get())
# end_run_marker_0
