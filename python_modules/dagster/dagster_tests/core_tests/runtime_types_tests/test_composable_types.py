from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterType,
    InputDefinition,
    OutputDefinition,
    Selector,
    check_dagster_type,
    dagster_type_loader,
    execute_solid,
    solid,
)
from dagster.core.types.loader_entry import (
    loader_from_entries,
    materializer_from_entries,
    type_builder,
    type_loader_entry,
    type_materializer_entry,
    with_loader_entry,
)


def do_check(_context, value):
    return isinstance(value, int)


@dagster_type_loader(Selector({"add_one": int, "add_two": int}))
def do_load(_, value):
    key, value = list(value.items())[0]
    if key == "add_one":
        return value + 1
    elif key == "add_two":
        return value + 2
    else:
        raise Exception("nope")


Inter = DagsterType(name="Inter", type_check_fn=do_check, loader=do_load)


@type_loader_entry(int)
def add_one(_, value):
    return value + 1


@type_loader_entry(int)
def add_two(_, value):
    return value + 2


@type_materializer_entry(int)
def add(_, config, value):
    return AssetMaterialization(AssetKey(("add", str(value + config))))


@type_materializer_entry(int)
def subtract(_, config, value):
    return AssetMaterialization(AssetKey(("subtract", str(value - config))))


Inter = DagsterType(
    name="Inter",
    type_check_fn=do_check,
    loader=loader_from_entries([add_one, add_two]),
    materializer=materializer_from_entries([add, subtract]),
)


def test_inter():
    assert check_dagster_type(Inter, 2)


def test_load():
    @solid(input_defs=[InputDefinition("inp", Inter)])
    def a_solid(_context, inp):
        return inp

    result = execute_solid(
        a_solid, run_config={"solids": {"a_solid": {"inputs": {"inp": {"add_one": 3}}}}}
    )

    assert result.success

    assert result.output_value() == 4

    result = execute_solid(
        a_solid, run_config={"solids": {"a_solid": {"inputs": {"inp": {"add_two": 3}}}}}
    )

    assert result.success

    assert result.output_value() == 5


def test_materialize():
    @solid(output_defs=[OutputDefinition(Inter)])
    def return_one(_context):
        return 2

    result = execute_solid(
        return_one, run_config={"solids": {"return_one": {"outputs": [{"result": {"add": 3}}]}}}
    )

    assert result.success

    mats = result.materializations_during_compute
    assert len(mats) == 1

    mat = mats[0]

    assert mat.asset_key == AssetKey(("add", "5"))

    result = execute_solid(
        return_one,
        run_config={"solids": {"return_one": {"outputs": [{"result": {"subtract": 1}}]}}},
    )

    assert result.success

    mats = result.materializations_during_compute
    assert len(mats) == 1

    mat = mats[0]

    assert mat.asset_key == AssetKey(("subtract", "1"))


def test_builder():
    @type_loader_entry(int)
    def add_three(_, value):
        return value + 3

    @with_loader_entry(add_three)
    @type_builder
    def inter_check(_, value):
        return isinstance(value, int)

    BuiltInter = inter_check.create_type("BuiltInter")

    # alternatively could get very cute like this, but PascalCasing functions
    # is problematic
    # @with_loader_entry(add_three)
    # @type_builder
    # def BuiltInter(_, value):
    #     return isinstance(value, int)

    @solid(input_defs=[InputDefinition("inp", BuiltInter)])
    def a_solid(_context, inp):
        return inp

    result = execute_solid(
        a_solid, run_config={"solids": {"a_solid": {"inputs": {"inp": {"add_three": 3}}}}}
    )

    assert result.success

    assert result.output_value() == 6
