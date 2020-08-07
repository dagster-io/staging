import subprocess
import sys

import pytest


def get_image_list():
    out = subprocess.check_output('dagster-image list', shell=True)
    return out.decode(sys.stdout.encoding).split()


def test_image_list():
    # Note: new images must be added here

    assert set(get_image_list()) == {
        'buildkite-integration',
        'buildkite-integration-base',
        'buildkite-integration-snapshot-builder',
        'buildkite-unit',
        'buildkite-unit-snapshot-builder',
        'coverage-image',
        'k8s-celery-worker',
        'k8s-example',
        'test-image-builder',
    }


@pytest.mark.parametrize('image_name', get_image_list())
def test_build_image(image_name):
    # Note: click.testing.CliRunner fails here with UnsupportedOperation: fileno

    subprocess.check_call('dagster-image build-all --name {}'.format(image_name), shell=True)
