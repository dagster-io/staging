from datetime import datetime
from typing import Callable, List, Optional

import pytest
from dagster.core.definitions.partition import (
    DynamicPartitionParams,
    Partition,
    StaticPartitionParams,
    TimeBasedPartitionParams,
)
from dagster.utils.partitions import DEFAULT_HOURLY_FORMAT_WITH_TIMEZONE


@pytest.mark.parametrize(
    argnames=["partitions"],
    argvalues=[([Partition("a_partition")],), ([Partition(x) for x in range(10)],)],
)
def test_static_partition_params(partitions: List[Partition]):
    partition_params = StaticPartitionParams(partitions)

    assert partition_params.get_partitions(current_time=None) == partitions


@pytest.mark.parametrize(
    argnames=[
        "start",
        "end",
        "delta_range",
        "fmt",
        "inclusive",
        "timezone",
        "expected_partitions",
    ],
    argvalues=[
        (
            datetime(year=2020, month=1, day=1),
            datetime(year=2020, month=1, day=6),
            "days",
            None,
            False,
            None,
            ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05"],
        ),
        (
            datetime(year=2020, month=1, day=1),
            datetime(year=2020, month=1, day=6, hour=1),
            "days",
            None,
            True,
            None,
            ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05", "2020-01-06"],
        ),
        (
            datetime(year=2020, month=12, day=29),
            datetime(year=2021, month=1, day=3),
            "days",
            None,
            False,
            None,
            ["2020-12-29", "2020-12-30", "2020-12-31", "2021-01-01", "2021-01-02"],
        ),
        (
            datetime(year=2020, month=2, day=28),
            datetime(year=2020, month=3, day=3),
            "days",
            None,
            False,
            None,
            ["2020-02-28", "2020-02-29", "2020-03-01", "2020-03-02"],
        ),
        (
            datetime(year=2019, month=2, day=28),
            datetime(year=2019, month=3, day=3),
            "days",
            None,
            False,
            None,
            ["2019-02-28", "2019-03-01", "2019-03-02"],
        ),
        (
            datetime(year=2019, month=2, day=28),
            datetime(year=2019, month=3, day=3),
            "days",
            None,
            False,
            None,
            ["2019-02-28", "2019-03-01", "2019-03-02"],
        ),
        (
            datetime(year=2020, month=1, day=1),
            datetime(year=2020, month=3, day=6),
            "months",
            None,
            False,
            None,
            ["2020-01-01", "2020-02-01"],
        ),
        (
            datetime(year=2020, month=1, day=1),
            datetime(year=2020, month=1, day=27),
            "weeks",
            None,
            False,
            None,
            ["2020-01-01", "2020-01-08", "2020-01-15"],
        ),
        (
            datetime(year=2020, month=12, day=1),
            datetime(year=2021, month=2, day=6),
            "months",
            None,
            False,
            None,
            ["2020-12-01", "2021-01-01"],
        ),
        (
            datetime(year=2020, month=2, day=12),
            datetime(year=2020, month=3, day=11),
            "months",
            None,
            False,
            None,
            [],
        ),
        # Daylight savings time partitions
        (
            datetime(year=2019, month=3, day=10, hour=1),
            datetime(year=2019, month=3, day=10, hour=4),
            "hours",
            DEFAULT_HOURLY_FORMAT_WITH_TIMEZONE,
            True,
            "UTC",
            [
                "2019-03-10-01:00+0000",
                "2019-03-10-02:00+0000",
                "2019-03-10-03:00+0000",
                "2019-03-10-04:00+0000",
            ],
        ),
        (
            datetime(year=2019, month=3, day=10, hour=1),
            datetime(year=2019, month=3, day=10, hour=4),
            "hours",
            DEFAULT_HOURLY_FORMAT_WITH_TIMEZONE,
            True,
            "UTC",
            [
                "2019-03-10-01:00+0000",
                "2019-03-10-02:00+0000",
                "2019-03-10-03:00+0000",
                "2019-03-10-04:00+0000",
            ],
        ),
        (
            datetime(year=2019, month=3, day=10, hour=1),
            datetime(year=2019, month=3, day=10, hour=4),
            "hours",
            DEFAULT_HOURLY_FORMAT_WITH_TIMEZONE,
            True,
            "US/Central",
            ["2019-03-10-01:00-0600", "2019-03-10-03:00-0500", "2019-03-10-04:00-0500"],
        ),
        (
            datetime(year=2019, month=11, day=3, hour=0),
            datetime(year=2019, month=11, day=3, hour=3),
            "hours",
            DEFAULT_HOURLY_FORMAT_WITH_TIMEZONE,
            True,
            "UTC",
            [
                "2019-11-03-00:00+0000",
                "2019-11-03-01:00+0000",
                "2019-11-03-02:00+0000",
                "2019-11-03-03:00+0000",
            ],
        ),
        (
            datetime(year=2019, month=11, day=3, hour=0),
            datetime(year=2019, month=11, day=3, hour=3),
            "hours",
            DEFAULT_HOURLY_FORMAT_WITH_TIMEZONE,
            True,
            "US/Central",
            [
                "2019-11-03-00:00-0500",
                "2019-11-03-01:00-0500",
                "2019-11-03-01:00-0600",
                "2019-11-03-02:00-0600",
                "2019-11-03-03:00-0600",
            ],
        ),
    ],
)
def test_time_based_partition_params(
    start: datetime,
    end: Optional[datetime],
    delta_range: str,
    fmt: Optional[str],
    inclusive: Optional[bool],
    timezone: Optional[str],
    expected_partitions: List[str],
):
    partition_params = TimeBasedPartitionParams(
        start=start,
        end=end,
        delta_range=delta_range,
        fmt=fmt,
        inclusive=inclusive,
        timezone=timezone,
    )

    generated_partitions = partition_params.get_partitions(current_time=None)

    assert all(
        isinstance(generated_partition, Partition) for generated_partition in generated_partitions
    )
    assert len(generated_partitions) == len(expected_partitions)
    assert all(
        generated_partition.name == expected_partition_name
        for generated_partition, expected_partition_name in zip(
            generated_partitions, expected_partitions
        )
    )


@pytest.mark.parametrize(
    argnames=["partition_fn"],
    argvalues=[
        (lambda _current_time: [Partition("a_partition")],),
        (lambda _current_time: [Partition(x) for x in range(10)],),
    ],
)
def test_dynamic_partitions(partition_fn: Callable[[Optional[datetime]], List[Partition]]):
    partition_params = DynamicPartitionParams(partition_fn)

    assert partition_params.get_partitions(current_time=None) == partition_fn(None)
