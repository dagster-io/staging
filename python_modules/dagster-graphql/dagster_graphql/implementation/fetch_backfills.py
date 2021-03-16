from .utils import capture_error


@capture_error
def get_backfill(graphene_info, backfill_id):
    from ..schema.backfill import GraphenePartitionBackfill

    if graphene_info.context.instance.has_bulk_actions_table():
        backfill_job = graphene_info.context.instance.get_backfill(backfill_id)
        return GraphenePartitionBackfill(backfill_id, backfill_job)
    else:
        return GraphenePartitionBackfill(backfill_id)


@capture_error
def get_backfills(graphene_info, cursor=None, limit=None):
    from ..schema.backfill import GraphenePartitionBackfill, GraphenePartitionBackfills

    backfills = graphene_info.context.instance.get_backfills(cursor=cursor, limit=limit)
    return GraphenePartitionBackfills(
        results=[
            GraphenePartitionBackfill(backfill.backfill_id, backfill) for backfill in backfills
        ]
    )
