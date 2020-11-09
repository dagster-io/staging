import inspect
import os

from dagster import check
from dagster.core.definitions.sensor import RunParams, RunSkippedData, SensorDefinition
from dagster.core.errors import DagsterInvariantViolationError
from dagster_aws.s3.sensor import s3_sensor


def directory_file_sensor(
    directory_name, pipeline_name, name=None, solid_selection=None, mode=None
):
    """
    Creates a sensor where the decorated function takes in the sensor context and a list of files
    that have been modified since the last sensor evaluation time.  The decorated sensor can do one
    of the following:

    1. Return a `RunSkippedData` object.
    2. Return a `RunParams` object.
    3. Yield a number of `RunParams` objects.

    Takes a :py:class:`~dagster.SensorExecutionContext`.

    Args:
        directory_name (str): The name of the directory on the filesystem to watch
        pipeline_name (str): The name of the pipeline to execute
        name (Optional[str]): The name of this sensor
        solid_selection (Optional[List[str]]): A list of solid subselection (including single
            solid names) to execute for runs for this sensor e.g.
            ``['*some_solid+', 'other_solid']``
        mode (Optional[str]): The mode to apply when executing runs for this sensor.
            (default: 'default')
    """

    def inner(fn):
        check.callable_param(fn, "fn")
        sensor_name = name or fn.__name__

        def _wrapped_fn(context):
            since = context.last_evaluation_time if context.last_evaluation_time else 0
            if not os.path.isdir(directory_name):
                yield RunSkippedData(f"Could not find directory named {directory_name}.")

            filenames_since = []
            for filename in os.listdir(directory_name):
                filepath = os.path.join(directory_name, filename)
                if not os.path.isfile(filepath):
                    continue
                fstats = os.stat(filepath)
                if fstats.st_mtime > since:
                    filenames_since.append(filename)

            result = fn(context, filenames_since)

            if inspect.isgenerator(result):
                for item in result:
                    yield item
            elif isinstance(result, (RunSkippedData, RunParams)):
                yield result

            elif result is not None:
                raise DagsterInvariantViolationError(
                    f"Error in sensor {sensor_name}: Sensor unexpectedly returned output "
                    f"{result} of type {type(result)}.  Should only return RunSkippedData or "
                    "RunParams objects."
                )

        return SensorDefinition(
            name=sensor_name,
            pipeline_name=pipeline_name,
            evaluation_fn=_wrapped_fn,
            solid_selection=solid_selection,
            mode=mode,
        )

    return inner


def get_toys_sensors():

    directory_name = os.environ.get("DAGSTER_TOY_SENSOR_DIRECTORY")

    @directory_file_sensor(directory_name=directory_name, pipeline_name="log_file_pipeline")
    def toy_file_sensor(_, modified_files):
        for filename in modified_files:
            yield RunParams(
                run_config={
                    "solids": {
                        "read_file": {"config": {"directory": directory_name, "filename": filename}}
                    }
                },
            )

    s3_bucket = os.environ.get("DAGSTER_AWS_SENSOR_BUCKET")

    @s3_sensor(pipeline_name="log_s3_pipeline", bucket=s3_bucket, default_interval=300)
    def aws_s3_log(modified_s3_objects):
        for obj in modified_s3_objects:
            yield RunParams(
                run_config={
                    "solids": {"log_path": {"config": {"bucket": s3_bucket, "path": obj["Key"]}}}
                },
                execution_key="{key}:{mtime}".format(
                    key=obj["Key"], mtime=str(obj["LastModified"])
                ),
            )

    return [toy_file_sensor, aws_s3_log]
