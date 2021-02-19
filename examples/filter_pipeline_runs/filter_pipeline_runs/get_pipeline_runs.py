from dagster import DagsterInstance, execute_pipeline, pipeline, solid
from dagster.core.storage.pipeline_run import PipelineRunStatus, PipelineRunsFilter


@solid(config_schema={"letter_number": str})
def hello_letter_number(context):
    return context.solid_config["letter_number"]


@pipeline
def get_letter_number_pipeline():
    hello_letter_number()


# start_run_marker_0
# As for PipelineRunStatus, below should cover all run statuses.
SUCCESS = PipelineRunStatus.SUCCESS
FAILURE = PipelineRunStatus.FAILURE
QUEUED = PipelineRunStatus.QUEUED
NOT_STARTED = PipelineRunStatus.NOT_STARTED
MANAGED = PipelineRunStatus.MANAGED
STARTING = PipelineRunStatus.STARTING
STARTED = PipelineRunStatus.STARTED
CANCELING = PipelineRunStatus.CANCELING
CANCELED = PipelineRunStatus.CANCELED


def get_pipeline_runs(instance, status_list):
    pipeline_runs = []

    execute_pipeline(
        get_letter_number_pipeline,
        instance=instance,
        run_config={"solids": {"hello_letter_number": {"config": {"letter_number": "A"}}}},
    )
    run_list = instance.get_runs(PipelineRunsFilter(statuses=status_list))

    for each_run in run_list:
        pipeline_runs.append(f"{each_run.pipeline_name} - {each_run.run_id}")

    return pipeline_runs


if __name__ == "__main__":
    statuses = [SUCCESS]
    get_pipeline_runs(DagsterInstance.get(), statuses)
# end_run_marker_0
