# pylint: disable=unused-argument

from dagster import Int, Output, OutputDefinition, SolidDefinition, solid


# start_a3235678029e11eb9775acde48001122
@solid
def my_solid(context):
    return 1


# end_a3235678029e11eb9775acde48001122
# start_a323a6b4029e11ebb0c8acde48001122


def _return_one(_context, inputs):
    yield Output(1)


solid = SolidDefinition(
    # end_a323a6b4029e11ebb0c8acde48001122
    name="my_solid",
    input_defs=[],
    output_defs=[OutputDefinition(Int)],
    compute_fn=_return_one,
)
