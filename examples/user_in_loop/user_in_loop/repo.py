import os
import time

from dagster import ModeDefinition, Output, OutputDefinition, pipeline, repository, solid


@solid
def get_price_list(_):
    return [10, 25, 40]


@solid(output_defs=[OutputDefinition(name="final_price_list", is_required=False)])
def add_tax(context, price_list: list):
    final_price_list = [(price + price * 0.0725) for price in price_list]
    context.log.info(f"The final price list is {final_price_list}")
    yield Output(final_price_list, "final_price_list")


@solid(config_schema={"file": str})
def wait_for_user_approval(context):
    while True:
        some_file = context.solid_config["file"]
        if os.path.isfile(some_file):
            context.log.info("Condition is met.")
            break
        context.log.info("Condition not met. Check again in five seconds.")
        time.sleep(5)


@pipeline
def user_in_the_loop_pipeline():
    price_list = get_price_list()
    wait_for_user_approval()
    add_tax(price_list)


@repository
def my_repo():
    return [user_in_the_loop_pipeline]
