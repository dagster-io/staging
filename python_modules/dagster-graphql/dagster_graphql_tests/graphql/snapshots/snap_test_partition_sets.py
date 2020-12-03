# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['TestPartitionSets.test_get_partition_sets_for_pipeline[readonly_in_memory_instance_in_process_env] 1'] = {
    'partitionSetsOrError': {
        '__typename': 'PartitionSets',
        'results': [
            {
                'mode': 'default',
                'name': 'integer_partition',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': [
                    'return_hello'
                ]
            },
            {
                'mode': 'default',
                'name': 'partition_based_decorator_partitions',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            },
            {
                'mode': 'default',
                'name': 'run_config_error_schedule_partitions',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            },
            {
                'mode': 'default',
                'name': 'scheduled_integer_partitions',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            },
            {
                'mode': 'default',
                'name': 'should_execute_error_schedule_partitions',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            },
            {
                'mode': 'default',
                'name': 'tags_error_schedule_partitions',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            },
            {
                'mode': 'default',
                'name': 'timezone_schedule_partitions',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            }
        ]
    }
}

snapshots['TestPartitionSets.test_get_partition_sets_for_pipeline[readonly_in_memory_instance_in_process_env] 2'] = {
    'partitionSetsOrError': {
        '__typename': 'PartitionSets',
        'results': [
        ]
    }
}

snapshots['TestPartitionSets.test_get_partition_sets_for_pipeline[readonly_in_memory_instance_multi_location] 1'] = {
    'partitionSetsOrError': {
        '__typename': 'PartitionSets',
        'results': [
            {
                'mode': 'default',
                'name': 'integer_partition',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': [
                    'return_hello'
                ]
            },
            {
                'mode': 'default',
                'name': 'partition_based_decorator_partitions',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            },
            {
                'mode': 'default',
                'name': 'run_config_error_schedule_partitions',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            },
            {
                'mode': 'default',
                'name': 'scheduled_integer_partitions',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            },
            {
                'mode': 'default',
                'name': 'should_execute_error_schedule_partitions',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            },
            {
                'mode': 'default',
                'name': 'tags_error_schedule_partitions',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            },
            {
                'mode': 'default',
                'name': 'timezone_schedule_partitions',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            }
        ]
    }
}

snapshots['TestPartitionSets.test_get_partition_sets_for_pipeline[readonly_in_memory_instance_multi_location] 2'] = {
    'partitionSetsOrError': {
        '__typename': 'PartitionSets',
        'results': [
        ]
    }
}
