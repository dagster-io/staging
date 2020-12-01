from dagster import ModeDefinition, execute_pipeline, executor, pipeline, solid
from dagster.core.execution.retries import Retries
from dagster.core.executor.in_process import InProcessExecutor


def test_in_process_executor_in_resources():
    solid_called = {}
    executor_called = {}

    class CustomInProcessExecutor(InProcessExecutor):
        def execute(self, pipeline_context, execution_plan):
            executor_called["yup"] = True
            return super(CustomInProcessExecutor, self).execute(pipeline_context, execution_plan)

    @executor(requires_multiprocess_safe_env=False)
    def some_executor(init_context):
        return CustomInProcessExecutor(
            retries=Retries.from_config(
                init_context.resource_config.get("retries", {"enabled": {}})
            ),
            marker_to_close=init_context.resource_config.get("marker_to_close"),
        )

    @solid
    def a_solid(_):
        solid_called["yup"] = True

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"in_process_executor": some_executor})])
    def a_pipe():
        a_solid()

    result = execute_pipeline(a_pipe)

    assert result.success
    assert solid_called["yup"]
    assert executor_called["yup"]
