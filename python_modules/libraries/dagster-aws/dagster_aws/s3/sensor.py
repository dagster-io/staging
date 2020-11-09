from datetime import datetime, timedelta

import boto3
from dagster import check
from dagster.core.definitions.sensor import SensorDefinition, SensorExecutionContext
from dagster.utils.backcompat import experimental

MAX_KEYS = 1000


@experimental
def s3_sensor(
    pipeline_name, bucket, prefix=None, solid_selection=None, mode=None, default_interval=60
):
    """Defines an S3 sensor takes in a list of S3 objects that have been modified since the last
    sensor evaluation, and returns a list of run params for runs that should be launched.  The
    default interval is used when the sensor has not been evaluated before.

    Args:
        pipeline_name (str): The name of the pipeline to execute when the sensor fires.
        bucket (str): The S3 bucket name
        prefix (Optional[str]): A string prefix to filter S3 paths
        solid_selection (Optional[List[str]]): A list of solid subselection (including single
            solid names) to execute when the sensor runs. e.g. ``['*some_solid+', 'other_solid']``
        mode (Optional[str]): The mode to apply when executing this sensor. (default: 'default')
        default_interval (Optional[int]): Number of seconds for the interval to check for
            modifications when the sensor is run for the first time
    """

    check.str_param(bucket, "bucket")
    check.opt_str_param(prefix, "prefix")
    check.opt_list_param(solid_selection, "solid_selection", of_type=str)
    check.opt_str_param(mode, "mode")
    check.opt_int_param(default_interval, "mode")

    def inner(fn):
        check.callable_param(fn, "fn")

        def _s3_updates(bucket, prefix, since):
            s3_session = boto3.resource("s3", use_ssl=True, verify=True).meta.client

            last_key = ""
            objects = []

            while True:
                response = s3_session.list_objects_v2(
                    Bucket=bucket,
                    Delimiter="",
                    MaxKeys=MAX_KEYS,
                    Prefix=prefix,
                    StartAfter=last_key,
                )
                objects.extend(
                    [obj for obj in response.get("Contents") if obj["LastModified"] > since]
                )

                if response["KeyCount"] < MAX_KEYS:
                    break

                last_key = response["Contents"][-1:]["Key"]

            return objects

        def _wrapped_fn(context):
            check.inst_param(context, "context", SensorExecutionContext)
            since = (
                context.last_evaluation_time
                if context.last_evaluation_time
                else datetime.utcnow() - timedelta(seconds=default_interval)
            )
            keys = _s3_updates(bucket, prefix, since=since)
            if not keys:
                return []

            return fn(keys)

        return SensorDefinition(
            name=fn.__name__,
            pipeline_name=pipeline_name,
            job_config_fn=_wrapped_fn,
            solid_selection=solid_selection,
            mode=mode,
        )

    return inner
