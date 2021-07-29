from dagster import AssetKey, EventRecordsFilter, RunRequest, asset_sensor, sensor

# @sensor(pipeline_name="story_recommender", mode="prod")
# def story_recommender_on_hn_table_update(context):
#     """Kick off a run if both the stories and comments tables have received updates"""
#     if context.last_run_key:
#         last_comments_key, last_stories_key = list(map(int, context.last_run_key.split("|")))
#     else:
#         last_comments_key, last_stories_key = None, None

#     comments_event_records = context.instance.get_event_records(
#         EventRecordsFilter(
#             asset_key=AssetKey(["snowflake", "hackernews", "comments"]),
#             after_cursor=last_comments_key,
#         ),
#         ascending=False,
#         limit=1,
#     )

#     stories_event_records = context.instance.get_event_records(
#         EventRecordsFilter(
#             asset_key=AssetKey(["snowflake", "hackernews", "stories"]),
#             after_cursor=last_stories_key,
#         ),
#         ascending=False,
#         limit=1,
#     )

#     if not comments_event_records or not stories_event_records:
#         return

#     last_comments_record_id = comments_event_records[0].storage_id
#     last_stories_record_id = stories_event_records[0].storage_id
#     run_key = f"{last_comments_record_id}|{last_stories_record_id}"

#     yield RunRequest(run_key=run_key)


@asset_sensor(
    pipeline_name="story_recommender",
    mode="prod",
    asset_keys=[
        AssetKey(["snowflake", "hackernews", "comments"]),
        AssetKey(["snowflake", "hackernews", "stories"]),
    ],
)
def story_recommender_on_hn_table_update(context, comment_records, story_records):
    """Kick off a run if both the stories and comments tables have received updates"""
    if not comment_records or not story_records:
        return

    comment_cursor = comment_records[0].storage_id
    story_cursor = story_records[0].storage_id

    run_key = f"{comment_cursor}|{story_cursor}"
    yield RunRequest(run_key=run_key)

    context.update_cursor(comment_cursor, story_cursor)
