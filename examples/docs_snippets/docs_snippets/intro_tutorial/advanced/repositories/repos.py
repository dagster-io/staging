"""isort:skip_file"""

import sys

from dagster import repository
from dagster.utils import script_relative_path

sys.path.append(script_relative_path("."))

from hello_cereal import hello_cereal_pipeline
from complex_pipeline import complex_pipeline


# start_a30c3740029e11eba303acde48001122
@repository
def hello_cereal_repository():
    return [hello_cereal_pipeline, complex_pipeline]


# end_a30c3740029e11eba303acde48001122
