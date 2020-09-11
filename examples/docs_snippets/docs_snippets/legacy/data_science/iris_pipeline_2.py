import dagstermill as dm
from six.moves.urllib.request import urlretrieve

from dagster import (
    Field,
    InputDefinition,
    Int,
    OutputDefinition,
    String,
    configured,
    pipeline,
    solid,
)
from dagster.utils import script_relative_path

# from docs_snippets.legacy.data_science.download_file import download_file



# from dagster.utils import script_relative_path


@solid(
    name="download_file",
    config_schema={
        "url": Field(String, description="The URL from which to download the file"),
        "path": Field(String, description="The path to which to download the file"),
    },
    output_defs=[
        OutputDefinition(
            String, name="path", description="The path to which the file was downloaded"
        )
    ],
    description=(
        "A simple utility solid that downloads a file from a URL to a path using "
        "urllib.urlretrieve"
    ),
)
def download_file(context):
    output_path = script_relative_path(context.solid_config["path"])
    context.log.info(script_relative_path("iris-kmeans_2.ipynb"))
    urlretrieve(context.solid_config["url"], output_path)
    return output_path


k_means_iris = dm.define_dagstermill_solid(
    "k_means_iris",
    script_relative_path("iris-kmeans_2.ipynb"),
    input_defs=[
        InputDefinition("path", str, description="Local path to the Iris dataset"),
        InputDefinition("secret_number", Int),
    ],
    config_schema={
        'seed': Field(Int, default_value=0, is_required=False, description='Seed for random array')
    },
)

# k_means_iris = configured(k_means_iris, name="k_means_iris")({"secret_number": 256100})


@pipeline
def iris_pipeline():
    k_means_iris(download_file())
