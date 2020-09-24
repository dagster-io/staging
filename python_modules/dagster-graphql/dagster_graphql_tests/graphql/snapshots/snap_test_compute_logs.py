# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['TestComputeLogs.test_get_compute_logs_over_graphql[sqlite_with_default_run_launcher_out_of_process_env] 1'] = {
    'stdout': {
        'data': '''2020-09-25 14:49:22 - dagster - DEBUG - spew_pipeline - 7ebb0480-9e6d-4554-b4c1-e9e1da2fb649 - 25159 - spew.compute - STEP_START - Started execution of step "spew.compute".
HELLO WORLD
2020-09-25 14:49:22 - dagster - DEBUG - spew_pipeline - 7ebb0480-9e6d-4554-b4c1-e9e1da2fb649 - 25159 - spew.compute - STEP_OUTPUT - Yielded output "result" of type "Any". (Type check passed).
2020-09-25 14:49:22 - dagster - DEBUG - spew_pipeline - 7ebb0480-9e6d-4554-b4c1-e9e1da2fb649 - 25159 - spew.compute - STEP_SUCCESS - Finished execution of step "spew.compute" in 26ms.
'''
    }
}

snapshots['TestComputeLogs.test_compute_logs_subscription_stdout_graphql[sqlite_with_default_run_launcher_out_of_process_env] 1'] = [
    {
        'computeLogs': {
            'data': '''2020-09-25 14:49:29 - dagster - DEBUG - spew_pipeline - 4c5b5386-defa-4a19-9edc-e692bcccb578 - 25176 - spew.compute - STEP_START - Started execution of step "spew.compute".
HELLO WORLD
2020-09-25 14:49:29 - dagster - DEBUG - spew_pipeline - 4c5b5386-defa-4a19-9edc-e692bcccb578 - 25176 - spew.compute - STEP_OUTPUT - Yielded output "result" of type "Any". (Type check passed).
2020-09-25 14:49:29 - dagster - DEBUG - spew_pipeline - 4c5b5386-defa-4a19-9edc-e692bcccb578 - 25176 - spew.compute - STEP_SUCCESS - Finished execution of step "spew.compute" in 25ms.
'''
        }
    }
]

snapshots['TestComputeLogs.test_compute_logs_subscription_stderr_graphql[sqlite_with_default_run_launcher_out_of_process_env] 1'] = [
    {
        'computeLogs': {
            'data': '''HELLO WORLD ERROR
'''
        }
    }
]
