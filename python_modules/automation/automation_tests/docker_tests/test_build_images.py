import subprocess
import sys

import pytest
from automation.docker.cli import build_all
from click.testing import CliRunner


def get_image_list():
    out = subprocess.check_output('dagster-image list', shell=True)
    return out.decode(sys.stdout.encoding).split()


@pytest.mark.parametrize('image_name', get_image_list())
def test_build_image(image_name):
    # runner = CliRunner()

    # result = runner.invoke(build_all, ['--name', image_name])

    # if result.exit_code != 0:
    #     raise Exception('Build failed. Result:\n{}'.format(result))

    # print(image_name)
    subprocess.check_call('dagster-image build-all --name {}'.format(image_name), shell=True)
