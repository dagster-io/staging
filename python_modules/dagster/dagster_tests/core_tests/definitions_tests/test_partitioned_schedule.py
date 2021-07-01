from datetime import datetime

import pendulum
from dagster import build_schedule_context, graph, solid
from dagster.core.definitions.partitioned_schedule import schedule_from_partitions
from dagster.core.definitions.time_window_partitions import TimeWindow, daily_partitioned_config

DATE_FORMAT = "%Y-%m-%d"


def time_window(start: str, end: str) -> TimeWindow:
    return TimeWindow(pendulum.parse(start), pendulum.parse(end))


def test_schedule_from_partitions():
    @daily_partitioned_config(start_date="2021-05-05")
    def my_partitioned_config(start, end):
        return {"start": str(start), "end": str(end)}

    @solid
    def my_solid():
        pass

    @graph
    def my_graph():
        my_solid()

    my_schedule = schedule_from_partitions(my_graph.to_job(config=my_partitioned_config))
    assert my_schedule.cron_schedule == "0 0 * * *"

    assert my_schedule.evaluate_tick(
        build_schedule_context(
            scheduled_execution_time=datetime.strptime("2021-05-08", DATE_FORMAT)
        )
    ).run_requests[0].run_config == {
        "start": "2021-05-07T00:00:00+00:00",
        "end": "2021-05-08T00:00:00+00:00",
    }
