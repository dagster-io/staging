# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['TestComputeLogs.test_get_compute_logs_over_graphql[sqlite_with_default_run_launcher_out_of_process_env] 1'] = {
    'stdout': {
        'data': '''2020-09-25 13:51:31 - dagster - DEBUG - spew_pipeline - 9fc8f598-5e5a-44f4-b636-9dedf57952f8 - 18577 - spew.compute - STEP_START - Started execution of step "spew.compute".
HELLO WORLD
2020-09-25 13:51:31 - dagster - DEBUG - spew_pipeline - 9fc8f598-5e5a-44f4-b636-9dedf57952f8 - 18577 - spew.compute - STEP_OUTPUT - Yielded output "result" of type "Any". (Type check passed).
2020-09-25 13:51:31 - dagster - DEBUG - spew_pipeline - 9fc8f598-5e5a-44f4-b636-9dedf57952f8 - 18577 - spew.compute - STEP_SUCCESS - Finished execution of step "spew.compute" in 28ms.
'''
    }
}

snapshots['TestComputeLogs.test_compute_logs_subscription_stdout_graphql[sqlite_with_default_run_launcher_out_of_process_env] 1'] = [
    {
        'computeLogs': {
            'data': '''2020-09-25 13:51:38 - dagster - DEBUG - spew_pipeline - 77688ed3-f540-4430-b3a3-049868605d21 - 18593 - spew.compute - STEP_START - Started execution of step "spew.compute".
HELLO WORLD
2020-09-25 13:51:38 - dagster - DEBUG - spew_pipeline - 77688ed3-f540-4430-b3a3-049868605d21 - 18593 - spew.compute - STEP_OUTPUT - Yielded output "result" of type "Any". (Type check passed).
2020-09-25 13:51:38 - dagster - DEBUG - spew_pipeline - 77688ed3-f540-4430-b3a3-049868605d21 - 18593 - spew.compute - STEP_SUCCESS - Finished execution of step "spew.compute" in 26ms.
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
