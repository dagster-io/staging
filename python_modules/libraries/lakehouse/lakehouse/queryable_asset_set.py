from dagster.core.selector.subset_selector import clause_to_subset, Traverser
from typing import Dict, Set, List
from lakehouse.asset import Asset


class QueryableAssetSet:
    def __init__(self, assets: List[Asset]):
        self._assets_by_path = {asset.path: asset for asset in assets}
        self._dep_graph = _generate_dep_graph(assets)

    def query_assets(self, query: str) -> List[Asset]:
        traverser = Traverser(graph=self._dep_graph)
        return clause_to_subset(traverser, self._dep_graph, query)


def _generate_dep_graph(assets) -> Dict[str, Dict[str, Set[str]]]:
    # defaultdict isn't appropriate because we also want to include items without dependencies
    graph = {'upstream': {}, 'downstream': {}}
    for asset in assets:
        graph['upstream'][asset.path] = set()
        if asset.computation:
            for dep in asset.computation.deps.values():
                graph['upstream'][asset.path].add(dep.asset.path)
                graph['downstream'][dep.asset.path].setdefault(set()).add(asset.path)

    return graph
