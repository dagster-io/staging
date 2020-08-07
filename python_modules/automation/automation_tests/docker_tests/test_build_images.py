import subprocess
import sys

import pytest


def get_image_list():
    out = subprocess.check_output('dagster-image list', shell=True)
    return out.decode(sys.stdout.encoding).split()


@pytest.mark.parametrize('image_name', get_image_list())
def test_build_image(image_name):
    # Note: click.testing.CliRunner fails here with UnsupportedOperation: fileno

    subprocess.check_call('dagster-image build-all --name {}'.format(image_name), shell=True)
