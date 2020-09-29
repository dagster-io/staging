from dagster import pipeline, solid


# start_a32afd2e029e11ebbc01acde48001122
@solid
def hello_logs(context):
    context.log.info("Hello, world!")


@pipeline
def demo_pipeline():
    hello_logs()


# end_a32afd2e029e11ebbc01acde48001122
