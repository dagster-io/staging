import random

from dagster import (
    AssetMaterialization,
    EventMetadataEntry,
    InputDefinition,
    Output,
    OutputDefinition,
    solid,
)
from pandas import DataFrame, Series
from scipy.sparse import coo_matrix
from sklearn.decomposition import TruncatedSVD


@solid
def build_recommender_model(_, author_story_matrix: coo_matrix) -> TruncatedSVD:
    """
    Trains an SVD model for collaborative filtering-based recommendation.
    """
    n_components = random.randint(90, 110)
    svd = TruncatedSVD(n_components=n_components)
    svd.fit(author_story_matrix)

    total_explained_variance = svd.explained_variance_ratio_.sum()

    yield AssetMaterialization(
        ["hackernews", "recommender_model"],
        metadata_entries=[
            EventMetadataEntry.float(total_explained_variance, "Total explained variance ratio"),
            EventMetadataEntry.int(n_components, "Number of components"),
        ],
    )
    yield Output(svd)


@solid(
    input_defs=[
        InputDefinition("model", dagster_type=TruncatedSVD),
        InputDefinition("col_stories", dagster_type=Series),
        InputDefinition(
            "story_titles",
            root_manager_key="gcp",
            metadata={
                "table": "bigquery-public-data.hacker_news.stories",
                "columns": ["id", "title"],
            },
            dagster_type=DataFrame,
        ),
    ],
    output_defs=[
        OutputDefinition(
            DataFrame,
            io_manager_key="warehouse",
            metadata={"table": "hackernews.component_top_stories"},
        )
    ],
)
def build_component_top_stories(
    _, model: TruncatedSVD, col_stories: Series, story_titles: DataFrame
):
    """
    For each component in the collaborative filtering model, finds the titles of the top stories
    it's associated with.
    """
    n_stories = 10

    components_column = []
    titles_column = []

    story_titles = story_titles.set_index("id")

    for i in range(model.components_.shape[0]):
        component = model.components_[i]
        top_story_indices = component.argsort()[-n_stories:][::-1]
        top_story_ids = col_stories[top_story_indices]
        top_story_titles = story_titles.loc[top_story_ids]

        for title in top_story_titles["title"]:
            components_column.append(i)
            titles_column.append(title)

    component_top_stories = DataFrame(
        {"component_index": Series(components_column), "title": Series(titles_column)}
    )

    yield Output(
        component_top_stories,
        metadata_entries=[
            EventMetadataEntry.md(
                top_components_to_markdown(component_top_stories), "Top component top stories"
            ),
        ],
    )


def top_components_to_markdown(component_top_stories: DataFrame) -> str:
    component_markdowns = []
    for i in range(5):
        component_i_top_5_stories = component_top_stories[
            component_top_stories["component_index"] == i
        ].head(5)

        component_markdowns.append(
            "\n".join(
                [f"Component {i}"]
                + ["- " + row["title"] for _, row in component_i_top_5_stories.iterrows()]
            )
        )

    return "\n\n".join(component_markdowns)
