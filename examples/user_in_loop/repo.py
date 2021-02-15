import json
import logging
import os
import time

from dagster import (
    Field,
    ModeDefinition,
    Output,
    OutputDefinition,
    logger,
    pipeline,
    repository,
    solid,
)


@logger(
    {
        "log_level": Field(str, is_required=False, default_value="INFO"),
        "name": Field(str, is_required=False, default_value="dagster"),
    },
    description="A JSON-formatted console logger",
)
def json_console_logger(init_context):
    level = init_context.logger_config["log_level"]
    name = init_context.logger_config["name"]

    klass = logging.getLoggerClass()
    new_logger = klass(name, level=level)

    handler = logging.StreamHandler()

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            return json.dumps(record.__dict__)

    handler.setFormatter(JsonFormatter())
    new_logger.addHandler(handler)

    return new_logger


@solid
def get_price_list(_):
    return [10, 25, 40]


@solid(output_defs=[OutputDefinition(name="final_price_list", is_required=False)])
def add_tax(context, price_list: list):
    final_price_list = [(price + price * 0.0725) for price in price_list]
    context.log.info(f"The final price list is {final_price_list}")
    yield Output(final_price_list, "final_price_list")


@solid
def wait_for_user_approval(context):
    while True:
        if os.path.isfile("/Users/a16502/dagster/examples/user_in_loop/data.csv"):
            context.log.info("Condition is met.")
            break
        context.log.info("Condition not met. Check again in five seconds.")
        time.sleep(5)


@pipeline(mode_defs=[ModeDefinition(logger_defs={"my_json_logger": json_console_logger})])
def user_in_the_loop_pipeline():
    price_list = get_price_list()
    wait_for_user_approval()
    add_tax(price_list)


@repository
def my_repo():
    return [user_in_the_loop_pipeline]
