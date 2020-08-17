# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['TestExecutionPlan.test_invalid_config_fetch_execute_plan[readonly_sqlite_instance_managed_grpc_env] 1'] = {
    'executionPlanOrError': {
        '__typename': 'PipelineConfigValidationInvalid',
        'errors': [
            {
                'message': 'Invalid scalar at path root:solids:sum_solid:inputs:num'
            }
        ],
        'pipelineName': 'csv_hello_world'
    }
}
