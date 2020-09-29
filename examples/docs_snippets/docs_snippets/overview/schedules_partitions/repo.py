from docs_snippets.overview.schedules_partitions.partition_definition import day_partition_set
from docs_snippets.overview.schedules_partitions.pipeline import my_pipeline

from dagster import repository


@repository
# start_a326b3c2029e11ebbd0aacde48001122
def my_repository():
    return [
        my_pipeline,
        day_partition_set,
    ]


# end_a326b3c2029e11ebbd0aacde48001122
