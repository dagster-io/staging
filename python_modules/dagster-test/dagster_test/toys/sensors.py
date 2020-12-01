import os

from dagster import AssetKey, check
from dagster.core.definitions.sensor import (
    RunRequest,
    SensorDefinition,
    SkipReason,
    wrap_sensor_evaluation,
)
from dagster.utils import utc_datetime_from_timestamp


def directory_file_sensor(
    directory_name, pipeline_name, name=None, solid_selection=None, mode=None
):
    """
    Creates a sensor where the decorated function takes in the sensor context and a list of files
    that have been modified since the last sensor evaluation time.  The decorated sensor can do one
    of the following:

    1. Return a `RunRequest` object.
    2. Yield multiple of `RunRequest` objects.
    3. Return or yield a `SkipReason` object, providing a descriptive message of why no runs were
       requested.
    4. Return or yield nothing (skipping without providing a reason)

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
            since = context.last_completion_time if context.last_completion_time else 0
            if not os.path.isdir(directory_name):
                yield SkipReason(f"Could not find directory named {directory_name}.")
                return

            fileinfo_since = []
            for filename in os.listdir(directory_name):
                filepath = os.path.join(directory_name, filename)
                if not os.path.isfile(filepath):
                    continue
                fstats = os.stat(filepath)
                if fstats.st_mtime > since:
                    fileinfo_since.append((filename, fstats.st_mtime))

            result = wrap_sensor_evaluation(sensor_name, fn(context, fileinfo_since))

            for item in result:
                yield item

        return SensorDefinition(
            name=sensor_name,
            pipeline_name=pipeline_name,
            evaluation_fn=_wrapped_fn,
            solid_selection=solid_selection,
            mode=mode,
        )

    return inner


def asset_sensor(asset_key, pipeline_name, name=None, solid_selection=None, mode=None):
    """
    Creates a sensor where the decorated function takes in the sensor context and the most recent
    update to a given asset key, including the event metdata.  The decorated sensor can do one of
    the following:

    1. Return a `RunRequest` object.
    2. Yield multiple of `RunRequest` objects.
    3. Return or yield a `SkipReason` object, providing a descriptive message of why no runs were
       requested.
    4. Return or yield nothing (skipping without providing a reason)

    Takes a :py:class:`~dagster.SensorExecutionContext`.

    Args:
        asset_key (AssetKey): The asset key of the asset to watch
        pipeline_name (str): The name of the pipeline to execute
        name (Optional[str]): The name of this sensor
        solid_selection (Optional[List[str]]): A list of solid subselection (including single
            solid names) to execute for runs for this sensor e.g.
            ``['*some_solid+', 'other_solid']``
        mode (Optional[str]): The mode to apply when executing runs for this sensor.
            (default: 'default')
    """
    check.inst_param(asset_key, "asset_key", AssetKey)
    check.str_param(pipeline_name, "pipeline_name")
    check.opt_str_param(name, "name")
    check.opt_str_param(solid_selection, "solid_selection")
    check.opt_str_param(mode, "mode")

    def inner(fn):
        check.callable_param(fn, "fn")
        sensor_name = name or fn.__name__

        def _wrapped_fn(context):
            since = utc_datetime_from_timestamp(context.last_completion_time or 0)
            if not context.instance.is_asset_aware:
                raise Exception("Instance is not asset-aware.")

            events = context.instance.events_for_asset_key(asset_key, since=since)
            if not events:
                return

            result = wrap_sensor_evaluation(
                sensor_name, fn(context, [event.dagster_event for event in events])
            )

            for item in result:
                yield item

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
    def toy_file_sensor(_, modified_fileinfo):
        for filename, mtime in modified_fileinfo:
            yield RunRequest(
                run_key="{}:{}".format(filename, str(mtime)),
                run_config={
                    "solids": {
                        "read_file": {"config": {"directory": directory_name, "filename": filename}}
                    }
                },
            )

    @asset_sensor(asset_key=AssetKey(["model"]), pipeline_name="log_asset_pipeline")
    def toy_asset_sensor(_, materialization_events):
        if not materialization_events:
            return

        event = materialization_events[0]  # just take the last one
        from_pipeline = event.pipeline_name

        yield RunRequest(
            run_key=None,
            run_config={
                "solids": {
                    "read_materialization": {
                        "config": {"asset_key": ["model"], "pipeline": from_pipeline}
                    }
                }
            },
        )

    return [toy_file_sensor, toy_asset_sensor]
