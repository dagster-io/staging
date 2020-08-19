import atexit
import json
import os
import subprocess
import time
from distutils import spawn

import pytest
from six.moves.urllib import request
from six.moves.urllib.error import URLError

from dagster.utils import file_relative_path, pushd
from dagster.utils.test.postgres_instance import TestPostgresInstance

DBT_EXECUTABLE = 'dbt'
TEST_PROJECT_DIR = file_relative_path(__file__, 'dagster_dbt_test_project')
DBT_CONFIG_DIR = os.path.join(TEST_PROJECT_DIR, 'dbt_config')
TEST_HOSTNAME = '127.0.0.1'
TEST_PORT = 8580
RPC_ESTABLISH_RETRIES = 4
RPC_ESTABLISH_RETRY_INTERVAL_S = 1.5


@pytest.fixture(scope='session')
def conn_string():
    with TestPostgresInstance.docker_service_up_or_skip(
        file_relative_path(__file__, 'docker-compose.yml'),
        'test-postgres-db-dbt',
        {'hostname': os.environ.get('POSTGRES_TEST_DB_DBT_HOST'),},
    ) as conn_str:
        yield conn_str


@pytest.fixture(scope='session')
def prepare_dbt_cli(conn_string):  # pylint: disable=unused-argument, redefined-outer-name
    if not spawn.find_executable(DBT_EXECUTABLE):
        raise Exception('executable not found in path for `dbt`')

    with pushd(TEST_PROJECT_DIR):
        yield


@pytest.fixture(scope='class')
def dbt_seed(prepare_dbt_cli):  # pylint: disable=unused-argument, redefined-outer-name
    subprocess.run([DBT_EXECUTABLE, 'seed', '--profiles-dir', DBT_CONFIG_DIR], check=True)


def get_rpc_server_status():
    status_request_body = b'{"jsonrpc": "2.0", "method": "status", "id": 1}'
    req = request.Request(
        'http://{hostname}:{port}/jsonrpc'.format(hostname=TEST_HOSTNAME, port=TEST_PORT),
        data=status_request_body,
        headers={'Content-type': 'application/json'},
    )
    resp = request.urlopen(req)
    return json.load(resp)


all_subprocs = set()


def kill_all_subprocs():
    for proc in all_subprocs:
        proc.kill()


atexit.register(kill_all_subprocs)


@pytest.fixture(scope='class')
def dbt_rpc_server(dbt_seed):  # pylint: disable=unused-argument, redefined-outer-name
    proc = subprocess.Popen(
        [
            DBT_EXECUTABLE,
            'rpc',
            '--host',
            TEST_HOSTNAME,
            '--port',
            str(TEST_PORT),
            '--profiles-dir',
            DBT_CONFIG_DIR,
        ],
    )

    # schedule to be killed in case of abort
    all_subprocs.add(proc)

    tries_remaining = RPC_ESTABLISH_RETRIES
    while True:
        poll_result = proc.poll()  # check on the child
        if poll_result != None:
            # terminated early!?
            raise Exception('DBT subprocess terminated before test could start.')

        try:
            status_json = get_rpc_server_status()
            if status_json['result']['state'] == 'ready':
                break
        except URLError:
            pass

        if tries_remaining <= 0:
            raise Exception('Exceeded max tries waiting for DBT RPC server to be ready.')
        tries_remaining -= 1
        time.sleep(RPC_ESTABLISH_RETRY_INTERVAL_S)

    yield

    proc.terminate()  # clean up after ourself
    proc.wait(timeout=0.2)
    if proc.poll() == None:  # still running
        proc.kill()
    all_subprocs.remove(proc)
