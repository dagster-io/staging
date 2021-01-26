import graphene

from .roots.mutation import Mutation
from .roots.query import Query
from .roots.subscription import Subscription


def create_schema():
    return graphene.Schema(query=Query, mutation=Mutation, subscription=Subscription)
