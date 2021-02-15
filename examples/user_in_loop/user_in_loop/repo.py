import os
import time

from dagster import Output, OutputDefinition, pipeline, repository, solid


# start_loop_marker_0
@solid
def get_price_list(_):
    return [5, 10, 12]


@solid(output_defs=[OutputDefinition(name="final_price_list", is_required=False)])
def add_tax(context, price_list: list):
    final_price_list = [(price + price * 0.03) for price in price_list]
    context.log.info(f"The final price list is {final_price_list}")
    yield Output(final_price_list, "final_price_list")


# end_loop_marker_0


# start_loop_marker_1
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


# end_loop_marker_1


@repository
def my_repo():
    return [user_in_the_loop_pipeline]
