from dagster import Field, Noneable, execute_solid, solid


def test_default_implies_not_required_field_correct():
    @solid(config_schema={"default_to_one": Field(int, default_value=1)})
    def return_default_to_one(context):
        return context.solid_config["default_to_one"]

    default_to_one_field = return_default_to_one.config_schema.config_type.fields["default_to_one"]
    assert default_to_one_field.is_required is False


def test_default_implies_not_required_execute_solid():
    @solid(config_schema={"default_to_one": Field(int, default_value=1)})
    def return_default_to_one(context):
        return context.solid_config["default_to_one"]

    # This currently throws. It should not require configuration as the
    # the default value should make it *not* required.
    execute_solid(return_default_to_one)


def test_scalar_field_defaults():
    assert Field(int).is_required is True
    assert Field(Noneable(int)).is_required is False
    assert Field(Noneable(int)).default_value is None


def test_noneable_shaped_field_defaults():
    schema = {"an_int": int}
    assert Field(schema).is_required is True
    assert Field(Noneable(schema)).is_required is False
    assert Field(Noneable(schema)).default_value is None
