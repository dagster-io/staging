from dagster import Field, execute_pipeline, graph, reconstructable, resource, solid


def define_resource(num):
    @resource(config_schema=Field(int, is_required=False))
    def a_resource(context):
        return num if context.resource_config is None else context.resource_config

    return a_resource


lots_of_resources = {"R" + str(r): define_resource(r) for r in range(20)}


@solid(required_resource_keys=set(lots_of_resources.keys()))
def all_resources():
    return 1


@solid(required_resource_keys={"R1"})
def one(context):
    return 1 + context.resources.R1


@solid(required_resource_keys={"R2"})
def two():
    return 1


@solid(required_resource_keys={"R1", "R2", "R3"})
def one_and_two_and_three():
    return 1


@graph
def resource_pipeline():
    all_resources()
    one()
    two()
    one_and_two_and_three()


resource_pipeline = resource_pipeline.to_job(lots_of_resources)

if __name__ == "__main__":
    result = execute_pipeline(
        reconstructable(resource_pipeline),
        run_config={
            "intermediate_storage": {"filesystem": {}},
            "execution": {"multiprocessing": {}},
        },
    )
