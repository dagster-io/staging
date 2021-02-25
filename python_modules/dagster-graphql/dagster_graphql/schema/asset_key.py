import graphene

from .util import non_null_list


class GrapheneAssetKey(graphene.ObjectType):
    path = non_null_list(graphene.String)

    class Meta:
        name = "AssetKey"


class GrapheneAssetRelation(graphene.ObjectType):
    assetKey = graphene.NonNull(GrapheneAssetKey)
    partitions = non_null_list(graphene.String)

    class Meta:
        name = "AssetRelation"