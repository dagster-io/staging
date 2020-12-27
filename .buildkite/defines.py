# This should be an enum once we make our own buildkite AMI with py3
class SupportedPython:
    V3_8 = "3.8.7"
    V3_7 = "3.7.9"
    V3_6 = "3.6.12"


SupportedPythons = [
    SupportedPython.V3_6,
    SupportedPython.V3_7,
    SupportedPython.V3_8,
]


TOX_MAP = {
    SupportedPython.V3_8: "py38",
    SupportedPython.V3_7: "py37",
    SupportedPython.V3_6: "py36",
}

####################################################################################################
# Dagster images
#
# These timestamps are generated with:
# datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S")
####################################################################################################

# Update this when releasing a new version of our integration image
# Per README.md, run the integration build image pipeline
# and then find the tag of the created images. A string
# like the following will be in that tag.
INTEGRATION_IMAGE_VERSION = "2020-12-27T184610"

# Update this when releasing a new version of our unit image
# Per README.md, run the unit build image pipeline
# and then find the tag of the created images. A string
# like the following will be in that tag.
UNIT_IMAGE_VERSION = "2020-12-11T192450"

COVERAGE_IMAGE_VERSION = "2020-12-27T203357"
