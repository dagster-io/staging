import pytest
from dagster import ModeDefinition, execute_solid, mem_io_manager
from hacker_news.solids.comment_stories import build_comment_stories
from pandas import DataFrame


@pytest.mark.parametrize(
    "comments, stories, expected",
    [
        ([[2, 1000, "bob"]], [[1000]], [[2, 1000, "bob"]]),
        ([[2, 1000, "bob"], [3, 2, "alice"]], [[1000]], [[2, 1000, "bob"], [3, 1000, "alice"]]),
    ],
)
def test_build_comment_stories(comments, stories, expected):
    comments = DataFrame(comments, columns=["id", "parent", "author"])
    stories = DataFrame(stories, columns=["id"])
    comment_stories = execute_solid(
        build_comment_stories,
        input_values={"stories": stories, "comments": comments},
        mode_def=ModeDefinition(resource_defs={"warehouse": mem_io_manager}),
    ).output_value()
    expected = DataFrame(expected, columns=["comment_id", "story_id", "commenter_id"]).set_index(
        "comment_id"
    )

    assert comment_stories.equals(expected)
