from dagster import RepositoryDefinition
from dagster.core.definitions.repository import RepositoryData


class LakehouseRepositoryDefinition(RepositoryDefinition):
    def __init__(self, name, assets):
        super(LakehouseRepositoryDefinition, self).__init__(name, RepositoryData.from_list([]))
        self._assets = assets
