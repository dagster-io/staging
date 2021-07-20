import datetime

from dagster import daily_schedule, pipeline, repository, solid


@solid(config_schema={"date": str})
def my_solid(context):
    return context.solid_config["date"]


@pipeline
def my_pipeline():
    my_solid()


@daily_schedule(pipeline_name="my_pipeline", start_date=datetime.datetime(2020, 1, 1))
def my_daily_schedule(date):
    return {"solids": {"my_solid": {"config": {"date": date.strftime("%Y-%m-%d %H")}}}}


@repository
def my_repo():
    return [my_pipeline, my_daily_schedule]
