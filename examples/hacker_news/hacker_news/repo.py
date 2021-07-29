from dagster import repository

from .pipelines.dbt_pipeline import dbt_pipeline
from .pipelines.download_pipeline import download_pipeline
from .pipelines.story_recommender import story_recommender
from .schedules.hourly_hn_download_schedule import hourly_hn_download_schedule
from .sensors.download_pipeline_finished_sensor import dbt_on_hn_download_finished
from .sensors.hn_tables_updated_sensor import story_recommender_on_hn_table_update
from .sensors.slack_on_pipeline_failure_sensor import make_pipeline_failure_sensor


@repository
def hn_download_repository():
    return [
        dbt_pipeline,
        download_pipeline,
        hourly_hn_download_schedule,
        dbt_on_hn_download_finished,
    ]


@repository
def hn_recommendation_repository():
    return [story_recommender, story_recommender_on_hn_table_update]
