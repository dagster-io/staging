import os

import yaml

from ..utils import git_repo_root


def get_image_version(image_name):
    root_images_path = os.path.join(
        git_repo_root(), "python_modules", "automation", "automation", "docker", "images",
    )

    with open(os.path.join(root_images_path, image_name, "last_updated.yaml")) as f:
        versions = set(yaml.safe_load(f).values())

    # There should be only one image timestamp tag across all Python versions
    assert len(versions) == 1
    return versions.pop()


COVERAGE_IMAGE_VERSION = get_image_version("buildkite-coverage")
INTEGRATION_IMAGE_VERSION = get_image_version("buildkite-integration")
UNIT_IMAGE_VERSION = get_image_version("buildkite-unit")
