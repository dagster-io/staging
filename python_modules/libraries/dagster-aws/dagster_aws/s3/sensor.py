from datetime import datetime

import boto3
from dagster import check

MAX_KEYS = 1000


def get_s3_updates(bucket, prefix="", since=None, s3_session=None):
    check.str_param(bucket, "bucket")
    check.str_param(prefix, "prefix")
    check.opt_inst_param(since, "since", datetime)

    if not s3_session:
        s3_session = boto3.resource("s3", use_ssl=True, verify=True).meta.client

    last_key = ""
    keys = []
    while True:
        response = s3_session.list_objects_v2(
            Bucket=bucket, Delimiter="", MaxKeys=MAX_KEYS, Prefix=prefix, StartAfter=last_key,
        )
        keys.extend(
            [
                obj.get("Key")
                for obj in response.get("Contents")
                if since is None or obj["LastModified"] > since
            ]
        )

        if response["KeyCount"] < MAX_KEYS:
            break

        last_key = response["Contents"][-1:]["Key"]

    return keys
