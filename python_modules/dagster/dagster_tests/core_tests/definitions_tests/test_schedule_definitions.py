import pytest
from dagster import ScheduleDefinition
from dagster.core.errors import DagsterInvalidDefinitionError


def test_bad_schedule_definition():
    with pytest.raises(DagsterInvalidDefinitionError, match="invalid cron schedule"):
        ScheduleDefinition("bad_schedule", "", pipeline_name="my_pipeline")

    with pytest.raises(DagsterInvalidDefinitionError, match="invalid cron schedule"):
        ScheduleDefinition("bad_schedule_two", "* * gibberish * *", pipeline_name="my_pipeline")

    ScheduleDefinition("good_schedule", "* * * * *", pipeline_name="my_pipeline")
    ScheduleDefinition("good_schedule_two", "*/5 * * * *", pipeline_name="my_pipeline")
