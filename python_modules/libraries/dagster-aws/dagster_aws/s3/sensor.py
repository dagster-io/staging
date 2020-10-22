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

    cursor = ""
    keys = []
    while True:
        response = s3_session.list_objects_v2(
            Bucket=bucket, Delimiter="", MaxKeys=MAX_KEYS, Prefix=prefix, StartAfter=cursor,
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

        cursor = response["Contents"][-1:]["Key"]

    return keys


def get_new_s3_keys(bucket, prefix="", last_key=None, s3_session=None):
    check.str_param(bucket, "bucket")
    check.str_param(prefix, "prefix")
    check.opt_str_param(last_key, "last_key")

    if not s3_session:
        s3_session = boto3.resource("s3", use_ssl=True, verify=True).meta.client

    cursor = ""
    contents = []

    while True:
        response = s3_session.list_objects_v2(
            Bucket=bucket, Delimiter="", MaxKeys=MAX_KEYS, Prefix=prefix, StartAfter=cursor,
        )
        contents.extend(response.get("Contents", []))
        if response["KeyCount"] < MAX_KEYS:
            break

        cursor = response["Contents"][-1]["Key"]

    sorted_keys = [obj["Key"] for obj in sorted(contents, key=lambda x: x["LastModified"])]

    if not last_key:
        return sorted_keys

    for idx, key in enumerate(sorted_keys):
        if key == last_key:
            return sorted_keys[idx + 1 :]

    return []
