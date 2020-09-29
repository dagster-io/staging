import json
from collections import namedtuple

from dagster import check
from dagster.serdes import whitelist_for_serdes


@whitelist_for_serdes
class Address(namedtuple("_Address", "path config_value")):
    """Pointer to a data object.
    Args:
        path (Optional[str]): A string that can be provided to a storage system to store/retrieve the
            outputted value if object_store is used.
        config_value (Optional[Dict[str, Any]]): the config_value to run the materialize method
            configured in dagster_type_materializer or the load method configured in
            dagster_type_loader.
    """

    def __new__(cls, path=None, config_value=None):
        check.invariant(path or config_value, "Either path or config_value must be provided")
        return super(Address, cls).__new__(cls, check.opt_str_param(path, "path"), config_value)

    @property
    def key(self):
        return self.path or json.dumps(self.config_value)
