import time

from dagster import RetryRequested, graph, op


@op
def echo(x):
    return x


@op(config_schema={"max_retries": int, "delay": float, "work_on_attempt": int})
def retry_solid(context) -> str:
    time.sleep(0.1)
    if (context.retry_number + 1) >= context.solid_config["work_on_attempt"]:
        return "success"
    else:
        raise RetryRequested(
            max_retries=context.solid_config["max_retries"],
            seconds_to_wait=context.solid_config["delay"],
        )


@graph
def retry_graph():
    echo(retry_solid())


retry_job = retry_graph.to_job(
    name="pass_after_retry_job",
    default_config={
        "solids": {
            "retry_solid": {
                "config": {
                    "delay": 0.2,
                    "work_on_attempt": 2,
                    "max_retries": 1,
                }
            }
        }
    },
)
