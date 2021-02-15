import os
import time

from dagster import pipeline, repository, solid

INITIAL_PRICE_LIST = [4, 9, 11]

# start_loop_marker_0
@solid(config_schema={"file_path": str})
def wait_for_condition_met(context):
    price_add_one = [(price + 1) for price in INITIAL_PRICE_LIST]

    while True:
        file_path = context.solid_config["file_path"]
        if os.path.exists(file_path):
            context.log.info("Condition is met.")
            break
        context.log.info("Condition not met. Check again soon.")
        time.sleep(1)

    final_price_list = [(price + price * 0.03) for price in price_add_one]
    context.log.info(f"The final price list is {final_price_list}")


@pipeline
def user_in_the_loop_pipeline():
    wait_for_condition_met()


# end_loop_marker_0


@repository
def my_repo():
    return [user_in_the_loop_pipeline]
