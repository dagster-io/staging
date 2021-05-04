import random

from dagster import Partition, PartitionSetDefinition, pipeline, repository, solid


# start_def
def get_date_partitions():
    """Every day in the month of May, 2020"""
    return [Partition(f"2020-05-{str(day).zfill(2)}") for day in range(1, 32)]


def run_config_for_date_partition(partition):
    date = partition.value
    return {"solids": {"process_data_for_date": {"config": {"date": date}}}}


date_partition_set = PartitionSetDefinition(
    name="date_partition_set",
    pipeline_name="my_data_pipeline",
    partition_fn=get_date_partitions,
    run_config_fn_for_partition=run_config_for_date_partition,
)
# end_def


@solid
def my_solid(_):
    pass


@pipeline
def my_data_pipeline():
    my_solid()


# start_repo_include
@repository
def my_repository():
    return [
        my_data_pipeline,
        date_partition_set,
    ]


# end_repo_include

my_partition_set = PartitionSetDefinition(
    name="my_partition_set",
    pipeline_name="my_data_pipeline",
    partition_fn=lambda: [
        Partition("a"),
        Partition("b"),
        Partition("c"),
    ],
    run_config_fn_for_partition=lambda _: {},
)

# start_manual_partition_schedule
def random_partition(_, partition_set_definition):
    # select a single, random partition
    partitions = partition_set_definition.get_partitions()
    return random.choice(partitions)


my_schedule = my_partition_set.create_schedule_definition(
    "my_schedule",
    "5 0 * * *",
    partition_selector=random_partition,
    execution_timezone="US/Eastern",
)


@repository
def my_repository_with_schedule():
    return [
        my_data_pipeline,
        my_partition_set,
        my_schedule,
    ]


# end_manual_partition_schedule
