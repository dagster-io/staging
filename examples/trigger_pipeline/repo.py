from dagster import execute_pipeline, pipeline, repository, solid


@solid
def add_one(_context, num: int) -> int:
    return num + 1


@solid
def add_two(_context, num: int) -> int:
    return num + 2


@solid
def subtract(_context, left: int, right: int) -> int:
    return left - right


@pipeline
def do_math():
    subtract(add_one(), add_two())


@repository
def my_repo():
    return [do_math]


# if __name__ == "__main__":
#     execute_pipeline(
#         do_math,
#         run_config={
#             "solids": {"add_one": {"inputs": {"num": 5}}, "add_two": {"inputs": {"num": 6}},}
#         },
#     )
