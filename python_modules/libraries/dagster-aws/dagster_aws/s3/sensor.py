import boto3
import os
import pytz
from datetime import datetime, timedelta
from dagster import check
from dagster.utils.backcompat import experimental
from dagster.core.definitions.sensor import SensorDefinition

MAX_KEYS = 1000


def _s3_updated(bucket, prefix, interval_seconds):
    s3_session = boto3.resource("s3", use_ssl=True, verify=True).meta.client
    modified_cutoff = datetime.utcnow().replace(tzinfo=pytz.UTC) - timedelta(
        seconds=interval_seconds
    )

    last_key = ''
    has_keys = False
    while True:
        response = s3_session.list_objects_v2(
            Bucket=bucket, Delimiter='', MaxKeys=MAX_KEYS, Prefix=prefix, StartAfter=last_key,
        )
        has_keys = any([obj['LastModified'] > modified_cutoff for obj in response.get('Contents')])

        if has_keys or response['KeyCount'] < MAX_KEYS:
            break

        last_key = response.get('Contents', [])[-1:].get('Key', '')

    return has_keys


@experimental
def s3_sensor(pipeline_name, bucket, prefix, sensor_name=None, mode="default"):
    check.str_param(pipeline_name, "pipeline_name")
    check.str_param(bucket, "bucket")
    check.opt_str_param(prefix, "prefix")
    check.opt_str_param(sensor_name, "sensor_name")
    check.opt_str_param(mode, "mode")

    def inner(fn):
        check.callable_param(fn, "fn")

        def _should_execute(_):
            return _s3_updated(bucket, prefix, interval_seconds=60)

        return SensorDefinition(
            name=sensor_name or fn.__name__,
            pipeline_name=pipeline_name,
            should_execute_fn=_should_execute,
            run_config_fn=fn,
            mode=mode,
            environment_vars={
                "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
                "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            },
        )

    return inner
