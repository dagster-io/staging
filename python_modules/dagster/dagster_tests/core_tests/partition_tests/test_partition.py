from datetime import datetime, time
from typing import Callable, List, Optional

import pytest
from dagster.check import CheckError
from dagster.core.definitions.partition import (
    DynamicPartitionParams,
    Partition,
    PartitionParams,
    StaticPartitionParams,
    TimeBasedPartitionParams,
)
from dagster.utils.partitions import DEFAULT_HOURLY_FORMAT_WITH_TIMEZONE


def assert_generated_partitions(partition_params: PartitionParams, expected_partitions: List[str]):
    generated_partitions = partition_params.get_partitions(current_time=None)

    assert all(
        isinstance(generated_partition, Partition) for generated_partition in generated_partitions
    )
    assert len(generated_partitions) == len(expected_partitions)
    for generated_partition, expected_partition_name in zip(
        generated_partitions, expected_partitions
    ):
        assert generated_partition.name == expected_partition_name


@pytest.mark.parametrize(
    argnames=["partitions"],
    argvalues=[([Partition("a_partition")],), ([Partition(x) for x in range(10)],)],
)
def test_static_partition_params(partitions: List[Partition]):
    partition_params = StaticPartitionParams(partitions)

    assert partition_params.get_partitions(current_time=None) == partitions


def test_time_partition_params_valid_start_and_end():
    with pytest.raises(CheckError, match=r"Selected date range start .* is after date range end"):
        TimeBasedPartitionParams(
            start=datetime(year=2021, month=1, day=3),
            end=datetime(year=2021, month=1, day=1),
        )


def test_time_partition_params_valid_execution_minute():
    with pytest.raises(CheckError, match=r"Execution minute .* must be between 0 and 59"):
        TimeBasedPartitionParams(
            start=datetime(year=2021, month=1, day=1),
            execution_minute=60,
        )


def test_time_partition_params_valid_execution_hour():
    with pytest.raises(CheckError, match=r"Execution hour .* must be between 0 and 23"):
        TimeBasedPartitionParams(
            start=datetime(year=2021, month=1, day=1),
            execution_hour=24,
        )


def test_time_partition_params_valid_execution_day_of_week():
    with pytest.raises(CheckError, match=r"Execution day of week .* must be between 0 and 6"):
        TimeBasedPartitionParams(
            start=datetime(year=2021, month=1, day=1),
            execution_day_of_week=7,
        )


def test_time_partition_params_valid_execution_day_of_month():
    with pytest.raises(CheckError, match=r"Execution day of month .* must be between 1 and 31"):
        TimeBasedPartitionParams(
            start=datetime(year=2021, month=1, day=1),
            execution_day_of_month=0,
        )


def test_time_partition_params_valid_execution_month():
    with pytest.raises(CheckError, match=r"Execution month .* must be between 1 and 12"):
        TimeBasedPartitionParams(
            start=datetime(year=2021, month=1, day=1),
            execution_month=0,
        )


def test_time_partition_params_valid_partition_minutes_offset():
    with pytest.raises(CheckError, match=r"Partition minutes offset .* must be greater than 0"):
        TimeBasedPartitionParams(
            start=datetime(year=2021, month=1, day=1),
            partition_minutes_offset=-1,
        )


def test_time_partition_params_valid_partition_hours_offset():
    with pytest.raises(CheckError, match=r"Partition hours offset .* must be greater than 0"):
        TimeBasedPartitionParams(
            start=datetime(year=2021, month=1, day=1),
            partition_hours_offset=-1,
        )


def test_time_partition_params_valid_partition_days_offset():
    with pytest.raises(CheckError, match=r"Partition days offset .* must be greater than 0"):
        TimeBasedPartitionParams(
            start=datetime(year=2021, month=1, day=1),
            partition_days_offset=-1,
        )


def test_time_partition_params_valid_partition_weeks_offset():
    with pytest.raises(CheckError, match=r"Partition weeks offset .* must be greater than 0"):
        TimeBasedPartitionParams(
            start=datetime(year=2021, month=1, day=1),
            partition_weeks_offset=-1,
        )


def test_time_partition_params_valid_partition_months_offset():
    with pytest.raises(CheckError, match=r"Partition months offset .* must be greater than 0"):
        TimeBasedPartitionParams(
            start=datetime(year=2021, month=1, day=1),
            partition_months_offset=-1,
        )


@pytest.mark.parametrize(
    argnames=[
        "start",
        "execution_minute",
        "execution_hour",
        "end",
        "partition_days_offset",
        "expected_partitions",
    ],
    ids=[
        "no partition days offset",
        "partition days offset == 1",
        "different start/end year",
        "leap year",
        "not leap year",
    ],
    argvalues=[
        (
            datetime(year=2021, month=1, day=1),
            20,
            1,
            datetime(year=2021, month=1, day=6, hour=1, minute=20),
            None,
            ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06"],
        ),
        (
            datetime(year=2021, month=1, day=1),
            20,
            1,
            datetime(year=2021, month=1, day=6, hour=1, minute=20),
            1,
            ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05"],
        ),
        (
            datetime(year=2020, month=12, day=29),
            20,
            1,
            datetime(year=2021, month=1, day=3, hour=1, minute=20),
            None,
            ["2020-12-29", "2020-12-30", "2020-12-31", "2021-01-01", "2021-01-02", "2021-01-03"],
        ),
        (
            datetime(year=2020, month=2, day=28),
            20,
            1,
            datetime(year=2020, month=3, day=3, hour=1, minute=20),
            None,
            ["2020-02-28", "2020-02-29", "2020-03-01", "2020-03-02", "2020-03-03"],
        ),
        (
            datetime(year=2021, month=2, day=28),
            20,
            1,
            datetime(year=2021, month=3, day=3, hour=1, minute=20),
            None,
            ["2021-02-28", "2021-03-01", "2021-03-02", "2021-03-03"],
        ),
    ],
)
def test_time_partition_params_daily_partitions(
    start: datetime,
    execution_minute: int,
    execution_hour: int,
    end: datetime,
    partition_days_offset: Optional[int],
    expected_partitions: List[str],
):
    partition_params = TimeBasedPartitionParams(
        start=start,
        execution_minute=execution_minute,
        execution_hour=execution_hour,
        end=end,
        partition_days_offset=partition_days_offset,
    )

    assert_generated_partitions(partition_params, expected_partitions)


@pytest.mark.parametrize(
    argnames=[
        "start",
        "end",
        "partition_months_offset",
        "expected_partitions",
    ],
    ids=[
        "no partition months offset",
        "partition months offset == 1",
        "execution day of month not within start/end range",
    ],
    argvalues=[
        (
            datetime(year=2021, month=1, day=1),
            datetime(year=2021, month=3, day=1, hour=1, minute=20),
            None,
            ["2021-01-01", "2021-02-01", "2021-03-01"],
        ),
        (
            datetime(year=2021, month=1, day=1),
            datetime(year=2021, month=3, day=1, hour=1, minute=20),
            1,
            ["2021-01-01", "2021-02-01"],
        ),
        (
            datetime(year=2021, month=1, day=3),
            datetime(year=2021, month=1, day=31),
            None,
            [],
        ),
    ],
)
def test_time_partition_params_monthly_partitions(
    start: datetime,
    end: datetime,
    partition_months_offset: Optional[int],
    expected_partitions: List[str],
):
    partition_params = TimeBasedPartitionParams(
        start=start,
        execution_minute=20,
        execution_hour=1,
        execution_day_of_month=1,
        end=end,
        partition_months_offset=partition_months_offset,
    )

    assert_generated_partitions(partition_params, expected_partitions)


@pytest.mark.parametrize(
    argnames=[
        "start",
        "end",
        "partition_weeks_offset",
        "expected_partitions",
    ],
    ids=[
        "no partition weeks offset",
        "partition weeks offset == 1",
        "execution day of week not within start/end range",
    ],
    argvalues=[
        (
            datetime(year=2021, month=1, day=1),
            datetime(year=2021, month=1, day=31, hour=1, minute=20),
            None,
            ["2021-01-03", "2021-01-10", "2021-01-17", "2021-01-24", "2021-01-31"],
        ),
        (
            datetime(year=2021, month=1, day=1),
            datetime(year=2021, month=1, day=31, hour=1, minute=20),
            1,
            ["2021-01-03", "2021-01-10", "2021-01-17", "2021-01-24"],
        ),
        (
            datetime(year=2021, month=1, day=4),
            datetime(year=2021, month=1, day=9),
            None,
            [],
        ),
    ],
)
def test_time_partition_params_weekly_partitions(
    start: datetime,
    end: datetime,
    partition_weeks_offset: Optional[int],
    expected_partitions: List[str],
):
    partition_params = TimeBasedPartitionParams(
        start=start,
        execution_minute=20,
        execution_hour=1,
        execution_day_of_week=0,
        end=end,
        partition_weeks_offset=partition_weeks_offset,
    )

    assert_generated_partitions(partition_params, expected_partitions)


@pytest.mark.parametrize(
    argnames=[
        "start",
        "end",
        "timezone",
        "partition_hours_offset",
        "expected_partitions",
    ],
    ids=[
        "no partition hours offset",
        "partition hours offset == 1",
        "execution hour not within start/end range",
        "Spring DST",
        "Spring DST with timezone",
        "Fall DST",
        "Fall DST with timezone",
    ],
    argvalues=[
        (
            datetime(year=2021, month=1, day=1, hour=0),
            datetime(year=2021, month=1, day=1, hour=4, minute=1),
            None,
            None,
            [
                "2021-01-01-00:01+0000",
                "2021-01-01-01:01+0000",
                "2021-01-01-02:01+0000",
                "2021-01-01-03:01+0000",
                "2021-01-01-04:01+0000",
            ],
        ),
        (
            datetime(year=2021, month=1, day=1, hour=0),
            datetime(year=2021, month=1, day=1, hour=4, minute=1),
            None,
            1,
            [
                "2021-01-01-00:01+0000",
                "2021-01-01-01:01+0000",
                "2021-01-01-02:01+0000",
                "2021-01-01-03:01+0000",
            ],
        ),
        (
            datetime(year=2021, month=1, day=1, hour=0, minute=2),
            datetime(year=2021, month=1, day=1, hour=0, minute=59),
            None,
            None,
            [],
        ),
        (
            datetime(year=2021, month=3, day=14, hour=1),
            datetime(year=2021, month=3, day=14, hour=4, minute=1),
            None,
            None,
            [
                "2021-03-14-01:01+0000",
                "2021-03-14-02:01+0000",
                "2021-03-14-03:01+0000",
                "2021-03-14-04:01+0000",
            ],
        ),
        (
            datetime(year=2021, month=3, day=14, hour=1),
            datetime(year=2021, month=3, day=14, hour=4, minute=1),
            "US/Central",
            None,
            ["2021-03-14-01:01-0600", "2021-03-14-03:01-0500", "2021-03-14-04:01-0500"],
        ),
        (
            datetime(year=2021, month=11, day=7, hour=0),
            datetime(year=2021, month=11, day=7, hour=4, minute=1),
            None,
            None,
            [
                "2021-11-07-00:01+0000",
                "2021-11-07-01:01+0000",
                "2021-11-07-02:01+0000",
                "2021-11-07-03:01+0000",
                "2021-11-07-04:01+0000",
            ],
        ),
        (
            datetime(year=2021, month=11, day=7, hour=0),
            datetime(year=2021, month=11, day=7, hour=4, minute=1),
            "US/Central",
            None,
            [
                "2021-11-07-00:01-0500",
                "2021-11-07-01:01-0500",
                "2021-11-07-01:01-0600",
                "2021-11-07-02:01-0600",
                "2021-11-07-03:01-0600",
                "2021-11-07-04:01-0600",
            ],
        ),
    ],
)
def test_time_partition_params_hourly_partitions(
    start: datetime,
    end: datetime,
    timezone: Optional[str],
    partition_hours_offset: Optional[int],
    expected_partitions: List[str],
):
    partition_params = TimeBasedPartitionParams(
        start=start,
        execution_minute=1,
        end=end,
        timezone=timezone,
        fmt=DEFAULT_HOURLY_FORMAT_WITH_TIMEZONE,
        partition_hours_offset=partition_hours_offset,
    )

    assert_generated_partitions(partition_params, expected_partitions)


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
