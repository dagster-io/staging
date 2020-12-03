from abc import ABC, abstractclassmethod, abstractproperty

from dagster import check


class ResourceMappableDefinition(ABC):
    @abstractproperty
    def required_resource_keys(self):
        """
        Returns the set of resource keys that this definition will be expecting a resource for.

        If an old key has been mapped to a new key, then in the mapped definition, only the new key
        would appear in the set of resource keys returned.

        Returns:
            FrozenSet[str]
        """
        raise NotImplementedError()

    @abstractproperty
    def resource_key_mappings(self):
        """
        Returns the underlying mappings that exist on this definition. If a key has been
        mapped, it will be represented with a ResourceKeyMapping object, if it is original, it will
        be a string.

        For example, consider a definition that originally has keys `{"foo", "bar"}`, and `"bar"` has
        been mapped to `"bar2"`. The expected output of this function would be the set
        `{"foo", ResourceKeyMapping(orig_key="bar", new_key="bar2")}`.

        Returns:
            FrozenSet[Union[str, ResourceKeyMapping]]
        """
        raise NotImplementedError()

    @abstractclassmethod
    def remap_resource_key(self, resource_key_mappings, name=None):
        """
        Applies provided resource key mappings to a new definition and returns that definition.

        Expects incoming resource mappings to map a key from this definition to a new key. The new
        definition will expect resources to be passed in with the new key.


        Args:
            resource_key_mappings (Dict[str, str]): Dictionary that represents key mappings. Maps an
                original key that is one of the `required_resource_keys` for this definition to a
                new_key which will take its place.

        Returns:
            ResourceMappableDefinition: A copy of this definition where resource keys have been
            replaced according to the provided mappings.

        **Examples**

        .. code-block:: python

            @solid(required_resource_keys={"foo"})
            def bar_solid(context):
                return context.resources.foo

            @resource
            def not_called_foo(_):
                return "Not Foo!"

            @pipeline(mode_defs=[ModeDefinition(resource_defs={"not_foo": not_called_foo})])
            def bar_pipeline():
                return bar_solid.remap_resource_key({"foo", "not_foo"})()

        """
        raise NotImplementedError()

    def get_mapped_resource_keys(self, resource_key_mappings):
        resource_key_mappings = check.dict_param(
            resource_key_mappings, "resource_key_mappings", key_type=str, value_type=str
        )

        check.invariant(
            all(
                [
                    resource_key in self.required_resource_keys
                    for resource_key in resource_key_mappings.keys()
                ]
            ),
            "Check that for each mapping passed in, the original key already exists in the set of "
            "resource keys.",
        )

        mapped_resource_keys = {
            vend_new_key(key_or_map)
            if vend_new_key(key_or_map) not in resource_key_mappings
            else ResourceKeyMapping(
                vend_old_key(key_or_map), resource_key_mappings[vend_new_key(key_or_map)]
            )
            for key_or_map in self.resource_key_mappings
        }
        return mapped_resource_keys


class ResourceKeyMapping:
    def __init__(self, orig_key, new_key):
        self._orig_key = check.str_param(orig_key, "orig_key")
        self._new_key = check.str_param(new_key, "new_key")

    @property
    def orig_key(self):
        return self._orig_key

    @property
    def new_key(self):
        return self._new_key

    def __eq__(self, other):
        if not isinstance(other, ResourceKeyMapping):
            return False
        return self.orig_key == other.orig_key and self.new_key == other.new_key

    def __hash__(self):
        return hash((self.orig_key, self.new_key))


def vend_new_key(str_or_resource_mapping):
    if isinstance(str_or_resource_mapping, ResourceKeyMapping):
        return str_or_resource_mapping.new_key
    else:
        return str_or_resource_mapping


def vend_old_key(str_or_resource_mapping):
    if isinstance(str_or_resource_mapping, ResourceKeyMapping):
        return str_or_resource_mapping.orig_key
    else:
        return str_or_resource_mapping
