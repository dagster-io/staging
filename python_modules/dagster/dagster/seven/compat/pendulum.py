from contextlib import contextmanager

import packaging.version
import pendulum

try:
    _IS_PENDULUM_2 = packaging.version.parse(pendulum.__version__).major >= 2  # type: ignore[union-attr]
except AttributeError:
    _IS_PENDULUM_2 = False


@contextmanager
def mock_pendulum_timezone(override_timezone):
    if _IS_PENDULUM_2:
        with pendulum.tz.test_local_timezone(  # pylint: disable=no-member
            pendulum.tz.timezone(override_timezone)  # pylint: disable=no-member
        ):

            yield
    else:
        with pendulum.tz.LocalTimezone.test(  # pylint: disable=no-member
            pendulum.Timezone.load(override_timezone)  # pylint: disable=no-member
        ):

            yield


def create_pendulum_time(year, month, day, *args, **kwargs):
    return (
        pendulum.datetime(  # pylint: disable=no-member, pendulum-create
            year, month, day, *args, **kwargs
        )
        if _IS_PENDULUM_2
        else pendulum.create(  # pylint: disable=no-member, pendulum-create
            year, month, day, *args, **kwargs
        )
    )


if _IS_PENDULUM_2:
    # type: ignore[attr-defined]
    PendulumDateTime = pendulum.DateTime  # pylint: disable=no-member
else:
    # type: ignore[attr-defined, misc]
    PendulumDateTime = pendulum.Pendulum  # pylint: disable=no-member

# Workaround for issues with .in_tz() in pendulum:
# https://github.com/sdispater/pendulum/issues/535
def to_timezone(dt, tz):
    from dagster import check

    check.inst_param(dt, "dt", PendulumDateTime)
    return pendulum.from_timestamp(dt.timestamp(), tz=tz)
