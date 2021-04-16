from dagster import InputDefinition, solid


def test_flex_inputs():
    @solid(input_defs=[InputDefinition("arg_b", metadata={"explicit": True})])
    def _partial(_context, arg_a, arg_b):
        return arg_a + arg_b
