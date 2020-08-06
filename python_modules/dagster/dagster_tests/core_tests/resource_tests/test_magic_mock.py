from dagster import ModeDefinition, execute_solid, resource, seven, solid


def test_magic_mock():
    things = []

    @resource
    def thing_sender(_):
        def _fn(thing):
            things.append(thing)

        return _fn

    @solid(required_resource_keys={'sender'})
    def send_thing(context):
        context.resources.sender('thing')

    prod_mode = ModeDefinition('prod', resource_defs={'sender': thing_sender})
    assert execute_solid(send_thing, prod_mode).success

    assert things == ['thing']

    things.clear()

    magic_mock_mode = ModeDefinition('magic_mock', resource_defs={'sender': resource.magic_mock})
    assert execute_solid(send_thing, magic_mock_mode).success

    assert things == []

    mock_obj = seven.mock.MagicMock()

    passed_mode = ModeDefinition('passed', resource_defs={'sender': resource.hardcoded(mock_obj)})

    assert execute_solid(send_thing, passed_mode).success

    mock_obj.assert_called_with('thing')
