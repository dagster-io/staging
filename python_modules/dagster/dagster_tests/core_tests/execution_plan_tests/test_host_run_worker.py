from dagster import ModeDefinition, pipeline, reconstructable, resource, solid
from dagster.core.definitions.reconstructable import ReconstructablePipeline
from dagster.core.execution.api import create_execution_plan, execute_run_host_mode
from dagster.core.test_utils import instance_for_test


@resource
def add_one_resource(_):
    def add_one(num):
        return num + 1

    return add_one


@resource
def add_two_resource(_):
    def add_two(num):
        return num + 2

    return add_two


@solid(required_resource_keys={"adder"})
def solid_that_uses_adder_resource(context, number):
    return context.resources.adder(number)


@pipeline(
    mode_defs=[
        ModeDefinition(name="add_one", resource_defs={"adder": add_one_resource}),
        ModeDefinition(name="add_two", resource_defs={"adder": add_two_resource}),
    ]
)
def pipeline_with_mode():
    solid_that_uses_adder_resource()


class ExplodingTestPipeline(ReconstructablePipeline):
    def __new__(cls, real_pipeline):  # pylint: disable=signature-differs
        return super(ExplodingTestPipeline, cls).__new__(
            cls,
            real_pipeline.repository,
            real_pipeline.pipeline_name,
            real_pipeline.solid_selection_str,
            real_pipeline.solids_to_execute,
        )

    def get_definition(self):
        raise Exception("Got the definition, no good")


def test_host_run_worker():
    with instance_for_test() as instance:
        run_config = {
            "solids": {"solid_that_uses_adder_resource": {"inputs": {"number": {"value": 4}}}},
            "intermediate_storage": {"filesystem": {}},
        }
        execution_plan = create_execution_plan(
            pipeline_with_mode,
            run_config,
        )

        pipeline_run = instance.create_run_for_pipeline(
            pipeline_def=pipeline_with_mode,
            execution_plan=execution_plan,
            run_config=run_config,
        )

        execute_run_host_mode(
            #            reconstructable(pipeline_with_mode),
            ExplodingTestPipeline(reconstructable(pipeline_with_mode)),
            pipeline_run,
            instance,
            raise_on_error=True,
        )
