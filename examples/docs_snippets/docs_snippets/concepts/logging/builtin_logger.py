from dagster import pipeline, repository, solid


# start_builtin_logger_marker_0
@solid
def hello_logs(context):
    context.log.info("Hello, world!")


# end_builtin_logger_marker_0


@pipeline
def demo_pipeline():
    hello_logs()


# start_builtin_logger_error_marker_0
@solid
def hello_logs_error(context):
    raise Exception("Somebody set up us the bomb")


@pipeline
def demo_pipeline_error():
    hello_logs_error()


# end_builtin_logger_error_marker_0


@repository
def repo():
    return [demo_pipeline, demo_pipeline_error]
