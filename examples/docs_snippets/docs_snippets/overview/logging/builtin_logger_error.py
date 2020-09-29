from dagster import pipeline, solid


# start_a32bb91c029e11ebac0facde48001122
@solid
def hello_logs_error(context):
    raise Exception("Somebody set up us the bomb")


@pipeline
def demo_pipeline_error():
    hello_logs_error()


# end_a32bb91c029e11ebac0facde48001122
