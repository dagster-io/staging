from dagster.config.validate import process_mappable_schema
from dagster.config.config_schema import MappedConfigSchema


def test_basic_config_mapping():

    mapped = MappedConfigSchema(
        outer_schema={"foo": int},
        inner_schema={"foo": int, "bar": int},
        resolvable_config=lambda outer: {"foo": outer["foo"], "bar": 2},
    )

    result = process_mappable_schema(mapped, {"foo": 3})

    assert result

    assert result.success

    assert result.value == {"foo": 3, "bar": 2}


def test_basic_value_config_mapping():

    mapped = MappedConfigSchema(
        outer_schema=None,
        inner_schema={"foo": int, "bar": int},
        resolvable_config=lambda outer: {"foo": 4, "bar": 2},
    )

    result = process_mappable_schema(mapped, {})

    assert result

    assert result.success

    assert result.value == {"foo": 4, "bar": 2}
