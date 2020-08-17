import collections

from dagster import (
    PythonObjectDagsterType,
    check_dagster_type,
    dagster_type,
    usable_as_dagster_type,
)
from dagster.core.types.dagster_type import resolve_dagster_type


def test_dagster_type_decorator():
    @usable_as_dagster_type(name=None)
    class Foo(object):
        pass

    @usable_as_dagster_type()
    class Bar(object):
        pass

    @usable_as_dagster_type
    class Baaz(object):
        pass

    assert resolve_dagster_type(Foo).name == 'Foo'
    assert resolve_dagster_type(Bar).name == 'Bar'
    assert resolve_dagster_type(Baaz).name == 'Baaz'


def test_dagster_type_decorator_name_desc():
    @usable_as_dagster_type(name='DifferentName', description='desc')
    class Something(object):
        pass

    resolved_type = resolve_dagster_type(Something)
    assert resolved_type.name == 'DifferentName'
    assert resolved_type.description == 'desc'


def test_make_dagster_type():
    SomeNamedTuple = collections.namedtuple('SomeNamedTuple', 'prop')
    DagsterSomeNamedTuple = PythonObjectDagsterType(SomeNamedTuple)
    resolved_type = resolve_dagster_type(DagsterSomeNamedTuple)
    assert resolved_type.name == 'SomeNamedTuple'
    assert SomeNamedTuple(prop='foo').prop == 'foo'

    DagsterNewNameNamedTuple = PythonObjectDagsterType(SomeNamedTuple, name='OverwriteName')
    resolved_type = resolve_dagster_type(DagsterNewNameNamedTuple)
    assert resolved_type.name == 'OverwriteName'


def test_python_object_type_with_custom_type_check():
    @dagster_type(name='Int3')
    def eq_3(_, value):
        return isinstance(value, int) and value == 3

    assert eq_3.name == 'Int3'
    assert check_dagster_type(eq_3, 3).success
    assert not check_dagster_type(eq_3, 5).success
