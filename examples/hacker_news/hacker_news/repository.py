from dagster import repository
from hacker_news.pipelines.story_recommender import story_recommender


@repository
def hacker_news():
    return [story_recommender]
