from dagster import check
from dagster.core.errors import DagsterBackfillFailedError
from dagster.core.execution.backfill import (
    BackfillJobStatus,
    PartitionBackfill,
    submit_backfill_runs,
)
from dagster.core.instance import DagsterInstance

MAX_RUNS_PER_ITERATION = 25


def execute_backfill_iteration(instance, logger, max_iteration_count=MAX_RUNS_PER_ITERATION):
    check.inst_param(instance, "instance", DagsterInstance)
    check.int_param(max_iteration_count, "max_iteration_count")

    if not instance.has_backfill_table():
        return

    backfill_jobs = instance.get_backfills(status=BackfillJobStatus.REQUESTED)

    if not backfill_jobs:
        logger.info("No backfill jobs requested.")
        return

    iteration_count = 0

    for backfill_job in backfill_jobs:
        backfill_id = backfill_job.backfill_id

        if not backfill_job.last_submitted_partition_name:
            logger.info(f"Starting backfill for {backfill_id}")
        else:
            logger.info(
                f"Resuming backfill for {backfill_id} from {backfill_job.last_submitted_partition_name}"
            )

        partition_names_chunk, has_more = _get_partitions_chunk(
            backfill_job, max_iteration_count - iteration_count
        )

        iteration_count = iteration_count + len(partition_names_chunk)

        try:
            submit_backfill_runs(instance, backfill_job, partition_names_chunk)
            if has_more:
                last_partition_name = partition_names_chunk[-1]
                instance.update_backfill(
                    backfill_job.with_partition_checkpoint(last_partition_name)
                )
            else:
                instance.update_backfill(backfill_job.with_status(BackfillJobStatus.COMPLETED))
        except DagsterBackfillFailedError as e:
            error_info = e.serializable_error_info
            instance.update_backfill(
                backfill_job.with_status(BackfillJobStatus.FAILED).with_error(error_info)
            )
            if error_info:
                logger.error(f"Backfill failed for {backfill_id}: {error_info.to_string()}")
                yield error_info

        if iteration_count >= max_iteration_count:
            return


def _get_partitions_chunk(backfill_job, chunk_size):
    check.inst_param(backfill_job, "backfill_job", PartitionBackfill)
    partition_names = backfill_job.partition_names
    if (
        backfill_job.last_submitted_partition_name
        and backfill_job.last_submitted_partition_name in partition_names
    ):
        index = partition_names.index(backfill_job.last_submitted_partition_name)
        partition_names = partition_names[index + 1 :]

    return partition_names[:chunk_size], chunk_size < len(partition_names)
