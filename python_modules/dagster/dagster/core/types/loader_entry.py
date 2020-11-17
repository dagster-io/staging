from dagster import check
from dagster.config import Selector

from .config_schema import dagster_type_loader, dagster_type_materializer


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
