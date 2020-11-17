from dagster import check
from dagster.config import Selector

from .config_schema import dagster_type_loader, dagster_type_materializer


class TypeBuilder:
    def __init__(self, fn):
        self.fn = fn
        self.loader_entries = []
        self.materializer_entries = []

    def add_loader_entry(self, loader_entry):
        self.loader_entries.append(loader_entry)
        return self

    def add_materializer_entry(self, materializer_entry):
        self.materializer_entries.append(materializer_entry)
        return self

    def create_type(self, name):
        from .dagster_type import DagsterType

        return DagsterType(
            name=name,
            type_check_fn=self.fn,
            loader=loader_from_entries(self.loader_entries) if self.loader_entries else None,
            materializer=materializer_from_entries(self.materializer_entries)
            if self.materializer_entries
            else None,
        )


def type_builder(fn):
    return TypeBuilder(fn)


def with_loader_entry(loader_entry):
    def _wrap(builder):
        return builder.add_loader_entry(loader_entry)

    return _wrap


class TypeLoaderEntry:
    def __init__(self, key, config_schema, fn):
        self.key = key
        self.config_schema = config_schema
        self.fn = fn


def loader_from_entries(type_loader_entries):
    selector_schema = {}
    fns = {}
    for entry in type_loader_entries:
        check.invariant(entry.key not in selector_schema)
        selector_schema[entry.key] = entry.config_schema

        check.invariant(entry.key not in fns)
        fns[entry.key] = entry.fn

    @dagster_type_loader(config_schema=Selector(selector_schema))
    def _do_load(context, value):
        key, value = list(value.items())[0]
        return fns[key](context, value)

    return _do_load


def type_loader_entry(config_schema, key=None):
    def _wrap(fn):
        return TypeLoaderEntry(key if key else fn.__name__, config_schema, fn)

    return _wrap


class TypeMaterializerEntry:
    def __init__(self, key, config_schema, fn):
        self.key = key
        self.config_schema = config_schema
        self.fn = fn


def materializer_from_entries(type_materializer_entries):
    selector_schema = {}
    fns = {}
    for entry in type_materializer_entries:
        check.invariant(entry.key not in selector_schema)
        selector_schema[entry.key] = entry.config_schema

        check.invariant(entry.key not in fns)
        fns[entry.key] = entry.fn

    @dagster_type_materializer(config_schema=Selector(selector_schema))
    def _do_load(context, config, value):
        key, config_for_entry = list(config.items())[0]
        return fns[key](context, config_for_entry, value)

    return _do_load


def type_materializer_entry(config_schema, key=None):
    def _wrap(fn):
        return TypeMaterializerEntry(key if key else fn.__name__, config_schema, fn)

    return _wrap
