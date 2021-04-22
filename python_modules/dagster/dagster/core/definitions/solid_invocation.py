def solid_invocation_result(solid_def, *args):

    # TODO: Assert that first arg passed in is solid execution context
    context = args[0]

    # TODO: should we accept inputs as positional? I think this is what people would expect given
    # the function signature
    inputs = args[1:]

    # TODO: better error message if someone provides wrong number of inputs
    input_dict = {}
    for input_def, input_value in zip(solid_def.input_defs, inputs):
        # TODO: type check
        input_dict[input_def.name] = input_value

    output_dict = {}
    for output in solid_def.compute_fn(context, input_dict):
        output_dict[output.output_name] = output.value

    return output_dict
