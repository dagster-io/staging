# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['TestReexecution.test_full_pipeline_reexecution_fs_storage[sqlite_with_default_run_launcher_managed_grpc_env] 1'] = {
    'launchPipelineExecution': {
        '__typename': 'LaunchPipelineRunSuccess',
        'run': {
            'mode': 'default',
            'pipeline': {
                'name': 'csv_hello_world'
            },
            'runConfigYaml': '<runConfigYaml dummy value>',
            'runId': '<runId dummy value>',
            'status': 'NOT_STARTED',
            'tags': [
                {
                    'key': '.dagster/grpc_info',
                    'value': '{"host": "localhost", "socket": "/var/folders/nr/y4m1jv5d7836kpcw4x2lb9rh0000gn/T/tmp5kru1h4t"}'
                }
            ]
        }
    }
}

snapshots['TestReexecution.test_full_pipeline_reexecution_in_memory_storage[sqlite_with_default_run_launcher_managed_grpc_env] 1'] = {
    'launchPipelineExecution': {
        '__typename': 'LaunchPipelineRunSuccess',
        'run': {
            'mode': 'default',
            'pipeline': {
                'name': 'csv_hello_world'
            },
            'runConfigYaml': '<runConfigYaml dummy value>',
            'runId': '<runId dummy value>',
            'status': 'NOT_STARTED',
            'tags': [
                {
                    'key': '.dagster/grpc_info',
                    'value': '{"host": "localhost", "socket": "/var/folders/nr/y4m1jv5d7836kpcw4x2lb9rh0000gn/T/tmpnygahh0j"}'
                }
            ]
        }
    }
}
