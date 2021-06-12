from dagster import (
    Float,
    InputDefinition,
    Int,
    List,
    ModeDefinition,
    OutputDefinition,
    composite_solid,
    pipeline,
    root_input_manager,
    solid,
)

# @solid(output_defs=[OutputDefinition(Int)])
# def emit_one(_):
#     return 1


# @solid(input_defs=[InputDefinition("numbers", List[Int])], output_defs=[OutputDefinition(Int)])
# def add(_, numbers):
#     return sum(numbers)


# @solid(input_defs=[InputDefinition("num", Float)], output_defs=[OutputDefinition(Float)])
# def div_two(_, num):
#     return num / 2


# @composite_solid(output_defs=[OutputDefinition(Int)])
# def emit_two():
#     return add([emit_one(), emit_one()])


# @composite_solid(input_defs=[InputDefinition("num", Int)], output_defs=[OutputDefinition(Int)])
# def add_four(num):
#     return add([emit_two(), emit_two(), num])


# @composite_solid(input_defs=[InputDefinition("num", Float)], output_defs=[OutputDefinition(Float)])
# def div_four(num):
#     return div_two(num=div_two(num))


# @solid(input_defs=[InputDefinition("num", Int)], output_defs=[OutputDefinition(Float)])
# def int_to_float(_, num):
#     return float(num)


# @pipeline(description="Demo pipeline that makes use of composite solids.")
# def composition():
#     div_four(int_to_float(add_four()))


@root_input_manager(input_config_schema={"test": str})
def my_root(context):
    return context.config["test"]


@solid(input_defs=[InputDefinition("data", dagster_type=str, root_manager_key="my_root")])
def inner_solid(_, data):
    return data


@composite_solid(input_defs=[InputDefinition("data", dagster_type=str)])
def my_composite_solid(data):
    _ = inner_solid(data=data)


@pipeline(mode_defs=[ModeDefinition(name="default", resource_defs={"my_root": my_root})])
def composition():
    my_composite_solid()
