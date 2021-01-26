import graphene

from ..errors import AssetNotFoundError, AssetsNotSupportedError, PythonError
from ..pipelines.pipeline import Asset
from ..util import non_null_list


class AssetConnection(graphene.ObjectType):
    nodes = non_null_list(Asset)


class AssetsOrError(graphene.Union):
    class Meta:
        types = (AssetConnection, AssetsNotSupportedError, PythonError)


class AssetOrError(graphene.Union):
    class Meta:
        types = (Asset, AssetsNotSupportedError, AssetNotFoundError)
