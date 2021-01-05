import dagstermill as dm
from dagster import ModeDefinition, fs_object_manager, pipeline
from dagster.utils import script_relative_path

k_means_iris = dm.define_dagstermill_solid(
    "k_means_iris", script_relative_path("iris-kmeans.ipynb")
)


@pipeline(mode_defs=[ModeDefinition(resource_defs={"object_manager": fs_object_manager})])
def iris_pipeline():
    k_means_iris()
