"""isort:skip_file"""
from docs_snippets.overview.object_managers.custom_object_manager import my_object_manager

# start_marker
from dagster import ModeDefinition, OutputDefinition, fs_object_manager, pipeline, solid


@solid(output_defs=[OutputDefinition(manager_key="db_object_manager")])
def solid1(_):
    """Return a Pandas DataFrame"""


@solid
def solid2(_, _input_dataframe):
    """Return some object"""


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={
                "object_manager": fs_object_manager,
                "db_object_manager": my_object_manager,  # defined in code snippet above
            }
        )
    ]
)
def my_pipeline():
    solid2(solid1())


# end_marker
