from dagster import ModeDefinition, fs_io_manager, pipeline
from hacker_news.resources.gcp_pandas_loader import gcp_to_pandas_loader
from hacker_news.resources.snowflake_io_manager import snowflake_io_manager
from hacker_news.solids.comment_stories import build_comment_stories
from hacker_news.solids.recommender_model import (
    build_component_top_stories,
    build_recommender_model,
)
from hacker_news.solids.user_story_matrix import build_user_story_matrix
from hacker_news.solids.user_top_recommended_stories import build_user_top_recommended_stories

DEV_MODE = ModeDefinition(
    "dev",
    description="This mode does all writes locally.",
    resource_defs={
        "gcp": gcp_to_pandas_loader,
        "io_manager": fs_io_manager,
        "warehouse": fs_io_manager,
    },
)

PROD_MODE = ModeDefinition(
    "prod",
    description="This mode writes some outputs to the production data warehouse.",
    resource_defs={
        "gcp": gcp_to_pandas_loader,
        "io_manager": fs_io_manager,
        "warehouse": snowflake_io_manager.configured(
            {
                "account": {"env": "SNOWFLAKE_ACCOUNT"},
                "user": {"env": "SNOWFLAKE_USER"},
                "password": {"env": "SNOWFLAKE_PASSWORD"},
                "database": "DEMO_DB",
                "warehouse": "TINY_WAREHOUSE",
            }
        ),
    },
)


@pipeline(
    mode_defs=[DEV_MODE, PROD_MODE],
    description="""
    Trains a collaborative filtering model that can recommend HN stories to users based on what
    stories they've commented on in the past.
    """,
)
def story_recommender():
    comment_stories = build_comment_stories()
    row_users, col_stories, user_story_matrix = build_user_story_matrix(comment_stories)
    recommender_model = build_recommender_model(user_story_matrix)
    build_component_top_stories(recommender_model, col_stories)
    build_user_top_recommended_stories(recommender_model, user_story_matrix, row_users, col_stories)
