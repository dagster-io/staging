import graphene


class MetadataItemDefinition(graphene.ObjectType):
    key = graphene.NonNull(graphene.String)
    value = graphene.NonNull(graphene.String)
