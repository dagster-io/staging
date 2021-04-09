import numpy as np
from dagster import ModeDefinition, execute_solid, mem_io_manager
from hacker_news.solids.user_top_recommended_stories import build_user_top_recommended_stories
from pandas import DataFrame, Series
from scipy.sparse import coo_matrix
from sklearn.decomposition import TruncatedSVD


def test_build_user_top_recommended_stories():
    model = TruncatedSVD()
    model.components_ = np.array([[1.0, 0.0, 1.0]])
    user_story_matrix = coo_matrix(np.array([[1.0, 0.0, 0.0]]))
    row_users = Series(["abc"])
    col_stories = Series([35, 38, 40])

    result = execute_solid(
        build_user_top_recommended_stories,
        input_values={
            "model": model,
            "user_story_matrix": user_story_matrix,
            "row_users": row_users,
            "col_stories": col_stories,
        },
        mode_def=ModeDefinition(resource_defs={"warehouse": mem_io_manager}),
    ).output_value()

    expected = DataFrame(
        [
            {"user_id": "abc", "story_id": 40, "relevance": 1.0},
            {"user_id": "abc", "story_id": 35, "relevance": 1.0},
        ]
    )

    assert result.equals(expected), str(result)
