from dagster import pipeline, solid


# start_a3271510029e11ebb738acde48001122
@solid(config_schema={"day_of_week": str})
def process_data_for_day(context):
    day_of_week = context.solid_config["day_of_week"]
    context.log.info(day_of_week)


@pipeline
def my_pipeline():
    process_data_for_day()


# end_a3271510029e11ebb738acde48001122
# start_a3287146029e11ebbb94acde48001122


@solid(config_schema={"date": str})
def process_data_for_date(context):
    date = context.solid_config["date"]
    context.log.info(date)


@pipeline
# end_a3287146029e11ebbb94acde48001122
def my_data_pipeline():
    process_data_for_date()
