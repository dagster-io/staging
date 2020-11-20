import os

from dagster.core.definitions.decorators.sensor import sensor
from dagster.core.definitions.sensor import SensorRunParams, SensorSkipData


def get_toys_sensors():
    directory_name = os.environ.get("DAGSTER_TOY_SENSOR_DIRECTORY")

    @sensor(pipeline_name="log_file_pipeline")
    def file_sensor_by_mtime(context):
        since = context.last_evaluation_time if context.last_evaluation_time else 0
        try:
            filenames = [
                filename
                for filename in os.listdir(directory_name)
                if os.path.isfile(os.path.join(directory_name, filename))
            ]
        except FileNotFoundError:
            yield SensorSkipData(
                "Environment variable `DAGSTER_TOY_SENSOR_DIRECTORY` is not set, skipping."
            )
            return

        for filename in filenames:
            fstats = os.stat(os.path.join(directory_name, filename))
            if fstats.st_mtime > since:
                yield SensorRunParams(
                    run_config={
                        "solids": {
                            "read_file": {
                                "config": {"directory": directory_name, "filename": filename}
                            }
                        }
                    },
                )

    @sensor(pipeline_name="log_file_pipeline")
    def file_sensor_by_execution_key(_):
        try:
            filenames = [
                filename
                for filename in os.listdir(directory_name)
                if os.path.isfile(os.path.join(directory_name, filename))
            ]
        except FileNotFoundError:
            yield SensorSkipData(
                "Environment variable `DAGSTER_TOY_SENSOR_DIRECTORY` is not set, skipping."
            )
            return

        for filename in filenames:
            fstats = os.stat(os.path.join(directory_name, filename))
            yield SensorRunParams(
                run_config={
                    "solids": {
                        "read_file": {"config": {"directory": directory_name, "filename": filename}}
                    }
                },
                execution_key="{}:{}".format(filename, str(fstats.st_mtime)),
            )

    return [file_sensor_by_mtime, file_sensor_by_execution_key]
