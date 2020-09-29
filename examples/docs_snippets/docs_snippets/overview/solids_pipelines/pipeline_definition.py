# pylint: disable=unused-argument

# start_a324493e029e11ebbbb1acde48001122
from dagster import DependencyDefinition, InputDefinition, PipelineDefinition, pipeline, solid


@solid
def return_one(context):
    return 1


@solid(input_defs=[InputDefinition("number", int)])
def add_one(context, number):
    return number + 1


@pipeline
def one_plus_one_pipeline():
    add_one(return_one())


# end_a324493e029e11ebbbb1acde48001122
# start_a324ad8c029e11eba782acde48001122


one_plus_one_pipeline_def = PipelineDefinition(
    name="one_plus_one_pipeline",
    solid_defs=[return_one, add_one],
    # end_a324ad8c029e11eba782acde48001122
    dependencies={"add_one": {"number": DependencyDefinition("return_one")}},
)
