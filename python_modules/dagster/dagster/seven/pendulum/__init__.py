# pylint: disable=no-member


from contextlib import contextmanager

import pendulum
import pkg_resources


def _is_pendulum_2():
    pendulum_version = pkg_resources.get_distribution("pendulum").version
    return pendulum_version.split(".")[0] == "2"


def datetime(year, month, day, *args, **kwargs):
    return (
        pendulum.datetime(year, month, day, *args, **kwargs)
        if _is_pendulum_2()
        else pendulum.datetime(year, month, day, *args, **kwargs)
    )


@contextmanager
def mock_pendulum_timezone(override_timezone):
    if _is_pendulum_2():
        with pendulum.tz.test_local_timezone(pendulum.tz.timezone(override_timezone)):
            yield
    else:
        with pendulum.tz.LocalTimezone.test(pendulum.Timezone.load(override_timezone)):
            yield


now = pendulum.now

test = pendulum.test

instance = pendulum.instance

period = pendulum.period

DateTime = pendulum.DateTime if _is_pendulum_2() else pendulum.Pendulum
