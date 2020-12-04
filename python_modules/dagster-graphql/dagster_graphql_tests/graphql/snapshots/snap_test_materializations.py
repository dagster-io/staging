# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['TestMaterializations.test_materializations[sqlite_with_sync_run_launcher_in_process_env] 1'] = [
    'PipelineStartingEvent',
    'PipelineStartEvent',
    'ExecutionStepStartEvent',
    'StepMaterializationEvent',
    'ExecutionStepOutputEvent',
    'ObjectStoreOperationEvent',
    'ExecutionStepSuccessEvent',
    'PipelineSuccessEvent'
]
