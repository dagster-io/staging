import subprocess

from dagster_spark.configs_spark import spark_config

from dagster import resource
from dagster.utils import frozendict

from .resources import PySparkResource


@resource({"spark_conf": spark_config()})
def pyspark_resource_custom(init_context):
    """A custom PySparkResource that sets `spark_conf.spark.driver.host` if not already set."""
    if init_context.resource_config["spark_conf"]["spark"]["driver"].get("host") is None:
        # Set `host` during resource initialization. Due to usage of `frozendict` (an immutable data
        # structure), this update requires verbose copying.
        init_context.log.info("Driver Host was not set. Setting Driver Host now.")
        driver = frozendict(
            {**init_context.resource_config["spark_conf"]["spark"]["driver"], "host": get_host_ip()}
        )
        spark = frozendict(
            {**init_context.resource_config["spark_conf"]["spark"], "driver": driver}
        )
        spark_conf = frozendict({**init_context.resource_config["spark_conf"], "spark": spark})
        resource_config = frozendict({**init_context.resource_config, "spark_conf": spark_conf})
        init_context = init_context.replace_config(resource_config)

    init_context.log.info(
        "Driver Host is now {}.".format(
            init_context.resource_config["spark_conf"]["spark"]["driver"]["host"]
        )
    )
    return PySparkResource(init_context.resource_config["spark_conf"])


def get_host_ip():
    """Helper function that runs `hostname-i` in the shell to get the IP address."""
    completed_process = subprocess.run(
        args="hostname -i", capture_output=True, encoding="utf8", check=True
    )
    return completed_process.stdout.strip()
