# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['test_create_noop_execution_plan 1'] = '''{
  "__class__": "ExecutionPlanSnapshot",
  "artifacts_persisted": false,
  "pipeline_snapshot_id": "4ec2812d4fc1c4da153f40dfd341209e71b327f7",
  "step_keys_to_execute": [
    "noop_solid.compute"
  ],
  "steps": [
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [],
      "key": "noop_solid.compute",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Any",
          "name": "result"
        }
      ],
      "solid_handle_id": "noop_solid"
    }
  ]
}'''

snapshots['test_create_execution_plan_with_dep 1'] = '''{
  "__class__": "ExecutionPlanSnapshot",
  "artifacts_persisted": false,
  "pipeline_snapshot_id": "a8a0e8854f17976452c4c819da057279efad5a1c",
  "step_keys_to_execute": [
    "solid_one.compute",
    "solid_two.compute"
  ],
  "steps": [
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [],
      "key": "solid_one.compute",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Any",
          "name": "result"
        }
      ],
      "solid_handle_id": "solid_one"
    },
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [
        {
          "__class__": "ExecutionStepInputSnap",
          "dagster_type_key": "Any",
          "name": "num",
          "upstream_output_handles": [
            {
              "__class__": "StepOutputHandle",
              "output_name": "result",
              "step_key": "solid_one.compute"
            }
          ]
        }
      ],
      "key": "solid_two.compute",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Any",
          "name": "result"
        }
      ],
      "solid_handle_id": "solid_two"
    }
  ]
}'''

snapshots['test_create_with_composite 1'] = '''{
  "__class__": "ExecutionPlanSnapshot",
  "artifacts_persisted": false,
  "pipeline_snapshot_id": "9f46013892a69c5eb6a733b0fa64a765ec9e93f4",
  "step_keys_to_execute": [
    "comp_1.return_one.compute",
    "comp_1.add_one.compute",
    "comp_2.return_one.compute",
    "comp_2.add_one.compute",
    "add.compute"
  ],
  "steps": [
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [
        {
          "__class__": "ExecutionStepInputSnap",
          "dagster_type_key": "Any",
          "name": "num_one",
          "upstream_output_handles": [
            {
              "__class__": "StepOutputHandle",
              "output_name": "result",
              "step_key": "comp_1.add_one.compute"
            }
          ]
        },
        {
          "__class__": "ExecutionStepInputSnap",
          "dagster_type_key": "Any",
          "name": "num_two",
          "upstream_output_handles": [
            {
              "__class__": "StepOutputHandle",
              "output_name": "result",
              "step_key": "comp_2.add_one.compute"
            }
          ]
        }
      ],
      "key": "add.compute",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Any",
          "name": "result"
        }
      ],
      "solid_handle_id": "add"
    },
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [
        {
          "__class__": "ExecutionStepInputSnap",
          "dagster_type_key": "Int",
          "name": "num",
          "upstream_output_handles": [
            {
              "__class__": "StepOutputHandle",
              "output_name": "out_num",
              "step_key": "comp_1.return_one.compute"
            }
          ]
        }
      ],
      "key": "comp_1.add_one.compute",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Int",
          "name": "result"
        }
      ],
      "solid_handle_id": "comp_1.add_one"
    },
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [],
      "key": "comp_1.return_one.compute",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Int",
          "name": "out_num"
        }
      ],
      "solid_handle_id": "comp_1.return_one"
    },
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [
        {
          "__class__": "ExecutionStepInputSnap",
          "dagster_type_key": "Int",
          "name": "num",
          "upstream_output_handles": [
            {
              "__class__": "StepOutputHandle",
              "output_name": "out_num",
              "step_key": "comp_2.return_one.compute"
            }
          ]
        }
      ],
      "key": "comp_2.add_one.compute",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Int",
          "name": "result"
        }
      ],
      "solid_handle_id": "comp_2.add_one"
    },
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [],
      "key": "comp_2.return_one.compute",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Int",
          "name": "out_num"
        }
      ],
      "solid_handle_id": "comp_2.return_one"
    }
  ]
}'''

snapshots['test_create_noop_execution_plan_with_tags 1'] = '''{
  "__class__": "ExecutionPlanSnapshot",
  "artifacts_persisted": false,
  "pipeline_snapshot_id": "ec9fc176554a4878c8c7e7f2c5a2772c2a6f048e",
  "step_keys_to_execute": [
    "noop_solid.compute"
  ],
  "steps": [
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [],
      "key": "noop_solid.compute",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [
        {
          "__class__": "ExecutionPlanMetadataItemSnap",
          "key": "bar",
          "value": "baaz"
        },
        {
          "__class__": "ExecutionPlanMetadataItemSnap",
          "key": "foo",
          "value": "bar"
        }
      ],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Any",
          "name": "result"
        }
      ],
      "solid_handle_id": "noop_solid"
    }
  ]
}'''
