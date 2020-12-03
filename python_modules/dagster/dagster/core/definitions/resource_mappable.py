from abc import ABC, abstractclassmethod, abstractproperty


class ResourceMappable(ABC):
    @abstractproperty
    def required_resource_keys(self):
        raise NotImplementedError()

    @abstractproperty
    def resource_mapping(self):
        raise NotImplementedError()

    @abstractclassmethod
    def copy_for_resource_mapping(self, orig_key, new_key, **kwargs):
        raise NotImplementedError()

    def map_resource_name_to_required_resource(self, orig_key, new_key, **kwargs):
        return self.copy_for_resource_mapping(orig_key, new_key, **kwargs)
