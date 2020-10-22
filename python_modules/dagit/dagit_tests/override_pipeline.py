from dagster import InputDefinition, Int, OutputDefinition, pipeline, repository, solid


@solid(input_defs=[InputDefinition("num", Int)], output_defs=[OutputDefinition(Int)])
def add_one(_, num):
    return num + 1


@solid(input_defs=[InputDefinition("num", Int)], output_defs=[OutputDefinition(Int)])
def mult_two(_, num):
    return num * 2


@pipeline
def math():
    return mult_two(num=add_one())


@repository
def test_override_repository():
    return [math]
