# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['test_create_execution_plan_with_dep 1'] = '''{
  "__class__": "ExecutionPlanSnapshot",
  "artifacts_persisted": true,
  "pipeline_snapshot_id": "3409017b30768ac4ec23361cbd84ccb9c084e237",
  "step_keys_to_execute": [
    "solid_one",
    "solid_two"
  ],
  "steps": [
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [],
      "is_resolved": true,
      "key": "solid_one",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Any",
          "name": "result",
          "output_def_snap": {
            "__class__": "OutputDefSnap",
            "dagster_type_display_name": "Any",
            "dagster_type_key": "Any",
            "description": null,
            "io_manager_key": "io_manager",
            "is_dynamic": false,
            "is_required": true,
            "name": "result"
          },
          "should_materialize": false,
          "solid_handle": {
            "__class__": "SolidHandle",
            "name": "solid_one",
            "parent": null
          }
        }
      ],
      "solid_handle_id": "solid_one",
      "step_handle": {
        "__class__": "StepHandle",
        "solid_handle": {
          "__class__": "SolidHandle",
          "name": "solid_one",
          "parent": null
        }
      },
      "tags": {}
    },
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [
        {
          "__class__": "ExecutionStepInputSnap",
          "dagster_type_key": "Any",
          "name": "num",
          "source": {
            "__class__": "FromStepOutput",
            "fan_in": false,
            "input_def_snap": {
              "__class__": "InputDefSnap",
              "dagster_type_key": "Any",
              "description": null,
              "name": "num",
              "root_manager_key": null
            },
            "input_solid_handle": {
              "__class__": "SolidHandle",
              "name": "solid_two",
              "parent": null
            },
            "step_output_handle": {
              "__class__": "StepOutputHandle",
              "mapping_key": null,
              "output_name": "result",
              "step_key": "solid_one"
            }
          },
          "upstream_output_handles": [
            {
              "__class__": "StepOutputHandle",
              "mapping_key": null,
              "output_name": "result",
              "step_key": "solid_one"
            }
          ]
        }
      ],
      "is_resolved": true,
      "key": "solid_two",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Any",
          "name": "result",
          "output_def_snap": {
            "__class__": "OutputDefSnap",
            "dagster_type_display_name": "Any",
            "dagster_type_key": "Any",
            "description": null,
            "io_manager_key": "io_manager",
            "is_dynamic": false,
            "is_required": true,
            "name": "result"
          },
          "should_materialize": false,
          "solid_handle": {
            "__class__": "SolidHandle",
            "name": "solid_two",
            "parent": null
          }
        }
      ],
      "solid_handle_id": "solid_two",
      "step_handle": {
        "__class__": "StepHandle",
        "solid_handle": {
          "__class__": "SolidHandle",
          "name": "solid_two",
          "parent": null
        }
      },
      "tags": {}
    }
  ]
}'''

snapshots['test_create_noop_execution_plan 1'] = '''{
  "__class__": "ExecutionPlanSnapshot",
  "artifacts_persisted": true,
  "pipeline_snapshot_id": "1f46d2b9d34fb73263aee58b40500bdab503ed44",
  "step_keys_to_execute": [
    "noop_solid"
  ],
  "steps": [
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [],
      "is_resolved": true,
      "key": "noop_solid",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Any",
          "name": "result",
          "output_def_snap": {
            "__class__": "OutputDefSnap",
            "dagster_type_display_name": "Any",
            "dagster_type_key": "Any",
            "description": null,
            "io_manager_key": "io_manager",
            "is_dynamic": false,
            "is_required": true,
            "name": "result"
          },
          "should_materialize": false,
          "solid_handle": {
            "__class__": "SolidHandle",
            "name": "noop_solid",
            "parent": null
          }
        }
      ],
      "solid_handle_id": "noop_solid",
      "step_handle": {
        "__class__": "StepHandle",
        "solid_handle": {
          "__class__": "SolidHandle",
          "name": "noop_solid",
          "parent": null
        }
      },
      "tags": {}
    }
  ]
}'''

snapshots['test_create_noop_execution_plan_with_tags 1'] = '''{
  "__class__": "ExecutionPlanSnapshot",
  "artifacts_persisted": true,
  "pipeline_snapshot_id": "3fdf1ba54d7ccdce0204eeb45d053ff5d4e96342",
  "step_keys_to_execute": [
    "noop_solid"
  ],
  "steps": [
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [],
      "is_resolved": true,
      "key": "noop_solid",
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
          "name": "result",
          "output_def_snap": {
            "__class__": "OutputDefSnap",
            "dagster_type_display_name": "Any",
            "dagster_type_key": "Any",
            "description": null,
            "io_manager_key": "io_manager",
            "is_dynamic": false,
            "is_required": true,
            "name": "result"
          },
          "should_materialize": false,
          "solid_handle": {
            "__class__": "SolidHandle",
            "name": "noop_solid",
            "parent": null
          }
        }
      ],
      "solid_handle_id": "noop_solid",
      "step_handle": {
        "__class__": "StepHandle",
        "solid_handle": {
          "__class__": "SolidHandle",
          "name": "noop_solid",
          "parent": null
        }
      },
      "tags": {
        "bar": "baaz",
        "foo": "bar"
      }
    }
  ]
}'''

snapshots['test_create_with_composite 1'] = '''{
  "__class__": "ExecutionPlanSnapshot",
  "artifacts_persisted": true,
  "pipeline_snapshot_id": "e4d7b87ce43e697233e24f420a0b586ebb7f4c7f",
  "step_keys_to_execute": [
    "comp_1.return_one",
    "comp_1.add_one",
    "comp_2.return_one",
    "comp_2.add_one",
    "add"
  ],
  "steps": [
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [
        {
          "__class__": "ExecutionStepInputSnap",
          "dagster_type_key": "Any",
          "name": "num_one",
          "source": {
            "__class__": "FromStepOutput",
            "fan_in": false,
            "input_def_snap": {
              "__class__": "InputDefSnap",
              "dagster_type_key": "Any",
              "description": null,
              "name": "num_one",
              "root_manager_key": null
            },
            "input_solid_handle": {
              "__class__": "SolidHandle",
              "name": "add",
              "parent": null
            },
            "step_output_handle": {
              "__class__": "StepOutputHandle",
              "mapping_key": null,
              "output_name": "result",
              "step_key": "comp_1.add_one"
            }
          },
          "upstream_output_handles": [
            {
              "__class__": "StepOutputHandle",
              "mapping_key": null,
              "output_name": "result",
              "step_key": "comp_1.add_one"
            }
          ]
        },
        {
          "__class__": "ExecutionStepInputSnap",
          "dagster_type_key": "Any",
          "name": "num_two",
          "source": {
            "__class__": "FromStepOutput",
            "fan_in": false,
            "input_def_snap": {
              "__class__": "InputDefSnap",
              "dagster_type_key": "Any",
              "description": null,
              "name": "num_two",
              "root_manager_key": null
            },
            "input_solid_handle": {
              "__class__": "SolidHandle",
              "name": "add",
              "parent": null
            },
            "step_output_handle": {
              "__class__": "StepOutputHandle",
              "mapping_key": null,
              "output_name": "result",
              "step_key": "comp_2.add_one"
            }
          },
          "upstream_output_handles": [
            {
              "__class__": "StepOutputHandle",
              "mapping_key": null,
              "output_name": "result",
              "step_key": "comp_2.add_one"
            }
          ]
        }
      ],
      "is_resolved": true,
      "key": "add",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Any",
          "name": "result",
          "output_def_snap": {
            "__class__": "OutputDefSnap",
            "dagster_type_display_name": "Any",
            "dagster_type_key": "Any",
            "description": null,
            "io_manager_key": "io_manager",
            "is_dynamic": false,
            "is_required": true,
            "name": "result"
          },
          "should_materialize": false,
          "solid_handle": {
            "__class__": "SolidHandle",
            "name": "add",
            "parent": null
          }
        }
      ],
      "solid_handle_id": "add",
      "step_handle": {
        "__class__": "StepHandle",
        "solid_handle": {
          "__class__": "SolidHandle",
          "name": "add",
          "parent": null
        }
      },
      "tags": {}
    },
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [
        {
          "__class__": "ExecutionStepInputSnap",
          "dagster_type_key": "Int",
          "name": "num",
          "source": {
            "__class__": "FromStepOutput",
            "fan_in": false,
            "input_def_snap": {
              "__class__": "InputDefSnap",
              "dagster_type_key": "Int",
              "description": null,
              "name": "num",
              "root_manager_key": null
            },
            "input_solid_handle": {
              "__class__": "SolidHandle",
              "name": "add_one",
              "parent": {
                "__class__": "SolidHandle",
                "name": "comp_1",
                "parent": null
              }
            },
            "step_output_handle": {
              "__class__": "StepOutputHandle",
              "mapping_key": null,
              "output_name": "out_num",
              "step_key": "comp_1.return_one"
            }
          },
          "upstream_output_handles": [
            {
              "__class__": "StepOutputHandle",
              "mapping_key": null,
              "output_name": "out_num",
              "step_key": "comp_1.return_one"
            }
          ]
        }
      ],
      "is_resolved": true,
      "key": "comp_1.add_one",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Int",
          "name": "result",
          "output_def_snap": {
            "__class__": "OutputDefSnap",
            "dagster_type_display_name": "Int",
            "dagster_type_key": "Int",
            "description": null,
            "io_manager_key": "io_manager",
            "is_dynamic": false,
            "is_required": true,
            "name": "result"
          },
          "should_materialize": false,
          "solid_handle": {
            "__class__": "SolidHandle",
            "name": "add_one",
            "parent": {
              "__class__": "SolidHandle",
              "name": "comp_1",
              "parent": null
            }
          }
        }
      ],
      "solid_handle_id": "comp_1.add_one",
      "step_handle": {
        "__class__": "StepHandle",
        "solid_handle": {
          "__class__": "SolidHandle",
          "name": "add_one",
          "parent": {
            "__class__": "SolidHandle",
            "name": "comp_1",
            "parent": null
          }
        }
      },
      "tags": {}
    },
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [],
      "is_resolved": true,
      "key": "comp_1.return_one",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Int",
          "name": "out_num",
          "output_def_snap": {
            "__class__": "OutputDefSnap",
            "dagster_type_display_name": "Int",
            "dagster_type_key": "Int",
            "description": null,
            "io_manager_key": "io_manager",
            "is_dynamic": false,
            "is_required": true,
            "name": "out_num"
          },
          "should_materialize": false,
          "solid_handle": {
            "__class__": "SolidHandle",
            "name": "return_one",
            "parent": {
              "__class__": "SolidHandle",
              "name": "comp_1",
              "parent": null
            }
          }
        }
      ],
      "solid_handle_id": "comp_1.return_one",
      "step_handle": {
        "__class__": "StepHandle",
        "solid_handle": {
          "__class__": "SolidHandle",
          "name": "return_one",
          "parent": {
            "__class__": "SolidHandle",
            "name": "comp_1",
            "parent": null
          }
        }
      },
      "tags": {}
    },
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [
        {
          "__class__": "ExecutionStepInputSnap",
          "dagster_type_key": "Int",
          "name": "num",
          "source": {
            "__class__": "FromStepOutput",
            "fan_in": false,
            "input_def_snap": {
              "__class__": "InputDefSnap",
              "dagster_type_key": "Int",
              "description": null,
              "name": "num",
              "root_manager_key": null
            },
            "input_solid_handle": {
              "__class__": "SolidHandle",
              "name": "add_one",
              "parent": {
                "__class__": "SolidHandle",
                "name": "comp_2",
                "parent": null
              }
            },
            "step_output_handle": {
              "__class__": "StepOutputHandle",
              "mapping_key": null,
              "output_name": "out_num",
              "step_key": "comp_2.return_one"
            }
          },
          "upstream_output_handles": [
            {
              "__class__": "StepOutputHandle",
              "mapping_key": null,
              "output_name": "out_num",
              "step_key": "comp_2.return_one"
            }
          ]
        }
      ],
      "is_resolved": true,
      "key": "comp_2.add_one",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Int",
          "name": "result",
          "output_def_snap": {
            "__class__": "OutputDefSnap",
            "dagster_type_display_name": "Int",
            "dagster_type_key": "Int",
            "description": null,
            "io_manager_key": "io_manager",
            "is_dynamic": false,
            "is_required": true,
            "name": "result"
          },
          "should_materialize": false,
          "solid_handle": {
            "__class__": "SolidHandle",
            "name": "add_one",
            "parent": {
              "__class__": "SolidHandle",
              "name": "comp_2",
              "parent": null
            }
          }
        }
      ],
      "solid_handle_id": "comp_2.add_one",
      "step_handle": {
        "__class__": "StepHandle",
        "solid_handle": {
          "__class__": "SolidHandle",
          "name": "add_one",
          "parent": {
            "__class__": "SolidHandle",
            "name": "comp_2",
            "parent": null
          }
        }
      },
      "tags": {}
    },
    {
      "__class__": "ExecutionStepSnap",
      "inputs": [],
      "is_resolved": true,
      "key": "comp_2.return_one",
      "kind": {
        "__enum__": "StepKind.COMPUTE"
      },
      "metadata_items": [],
      "outputs": [
        {
          "__class__": "ExecutionStepOutputSnap",
          "dagster_type_key": "Int",
          "name": "out_num",
          "output_def_snap": {
            "__class__": "OutputDefSnap",
            "dagster_type_display_name": "Int",
            "dagster_type_key": "Int",
            "description": null,
            "io_manager_key": "io_manager",
            "is_dynamic": false,
            "is_required": true,
            "name": "out_num"
          },
          "should_materialize": false,
          "solid_handle": {
            "__class__": "SolidHandle",
            "name": "return_one",
            "parent": {
              "__class__": "SolidHandle",
              "name": "comp_2",
              "parent": null
            }
          }
        }
      ],
      "solid_handle_id": "comp_2.return_one",
      "step_handle": {
        "__class__": "StepHandle",
        "solid_handle": {
          "__class__": "SolidHandle",
          "name": "return_one",
          "parent": {
            "__class__": "SolidHandle",
            "name": "comp_2",
            "parent": null
          }
        }
      },
      "tags": {}
    }
  ]
}'''
