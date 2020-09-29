from dagster import Partition, PartitionSetDefinition


# start_a327854a029e11eb9ee2acde48001122
def get_day_partition():
    return [
        Partition("M"),
        Partition("Tu"),
        Partition("W"),
        Partition("Th"),
        Partition("F"),
        Partition("Sa"),
        Partition("Su"),
    ]


def run_config_for_day_partition(partition):
    day = partition.value
    return {"solids": {"process_data_for_day": {"config": {"day_of_week": day}}}}


# start_a32637b4029e11eb955aacde48001122
day_partition_set = PartitionSetDefinition(
    name="day_partition_set",
    pipeline_name="my_pipeline",
    partition_fn=get_day_partition,
    run_config_fn_for_partition=run_config_for_day_partition,
    # end_a327854a029e11eb9ee2acde48001122
)
# end_a32637b4029e11eb955aacde48001122
