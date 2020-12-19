import pytest
from dagster.utils import file_relative_path
from dagster.utils.test.mysql_instance import TestMySQLInstance
from dagster_mysql.run_storage import MySQLRunStorage
from dagster_mysql.schedule_storage.schedule_storage import MySQLScheduleStorage


@pytest.fixture(scope="function")
def hostname(conn_string):  # pylint: disable=redefined-outer-name, unused-argument
    return TestMySQLInstance.get_hostname()


@pytest.fixture(scope="function")
def conn_string():  # pylint: disable=redefined-outer-name, unused-argument
    with TestMySQLInstance.docker_service_up_or_skip(
        file_relative_path(__file__, "docker-compose.yml"), "test-mysql-db"
    ) as conn_str:
        yield conn_str


@pytest.fixture(scope="function")
def clean_storage(conn_string):  # pylint: disable=redefined-outer-name
    storage = MySQLRunStorage.create_clean_storage(conn_string)
    assert storage
    return storage


@pytest.fixture(scope="function")
def clean_schedule_storage(conn_string):  # pylint: disable=redefined-outer-name
    storage = MySQLScheduleStorage.create_clean_storage(conn_string)
    assert storage
    return storage
