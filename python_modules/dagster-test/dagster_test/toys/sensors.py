import os

from dagster import AssetKey, check
from dagster.core.definitions.sensor import (
    RunRequest,
    SensorDefinition,
    SkipReason,
    wrap_sensor_evaluation,
)


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


def asset_sensor(
    asset_key, pipeline_name, name=None, solid_selection=None, mode=None, run_key_to_cursor=None
):
    """
    Creates a sensor where the decorated function takes in the sensor context and a list of
    materialization event data, including record ids.

    The decorated sensor can do one of the following:

    1. Return a `RunRequest` object.
    2. Yield multiple of `RunRequest` objects.
    3. Return or yield a `SkipReason` object, providing a descriptive message of why no runs were
       requested.
    4. Return or yield nothing (skipping without providing a reason)

    Args:
        asset_key (AssetKey): The asset key of the asset to watch
        pipeline_name (str): The name of the pipeline to execute
        name (Optional[str]): The name of this sensor
        solid_selection (Optional[List[str]]): A list of solid subselection (including single
            solid names) to execute for runs for this sensor e.g.
            ``['*some_solid+', 'other_solid']``
        mode (Optional[str]): The mode to apply when executing runs for this sensor.
            (default: 'default')
        run_key_to_cursor (Optional[Callable[[str], str]]) A mapping function from the last
            completed run_key to an event storage cursor
    """
    check.inst_param(asset_key, "asset_key", AssetKey)
    check.str_param(pipeline_name, "pipeline_name")
    check.opt_str_param(name, "name")
    check.opt_str_param(solid_selection, "solid_selection")
    check.opt_str_param(mode, "mode")
    run_key_to_cursor = check.opt_callable_param(
        run_key_to_cursor, "run_key_to_cursor", lambda run_key: int(run_key) if run_key else None
    )

    def inner(fn):
        check.callable_param(fn, "fn")
        sensor_name = name or fn.__name__

        def _wrapped_fn(context):
            if not context.instance.is_asset_aware:
                raise Exception("Instance is not asset-aware.")

            cursor = run_key_to_cursor(context.last_run_key)
            events = context.instance.events_for_asset_key(asset_key, cursor=cursor, ascending=True)
            if not events:
                return

            result = wrap_sensor_evaluation(
                sensor_name,
                fn(
                    context,
                    [
                        tuple([str(record_id), event.dagster_event])
                        for record_id, event in reversed(events)
                    ],
                ),
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
        if not modified_fileinfo:
            yield SkipReason("No modified files")

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

        run_key, event = materialization_events[0]  # take the most recent materialization
        from_pipeline = event.pipeline_name

        yield RunRequest(
            run_key=run_key,
            run_config={
                "solids": {
                    "read_materialization": {
                        "config": {"asset_key": ["model"], "pipeline": from_pipeline}
                    }
                }
            },
        )

    return [toy_file_sensor, toy_asset_sensor]
