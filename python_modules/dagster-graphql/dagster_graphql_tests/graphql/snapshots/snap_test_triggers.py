# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_in_process_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinition',
        'mode': 'default',
        'name': 'triggered_no_config',
        'pipelineName': 'no_config_pipeline',
        'runConfigOrError': {
        },
        'solidSelection': None,
        'tagsOrError': {
        }
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_in_process_env] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_out_of_process_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinition',
        'mode': 'default',
        'name': 'triggered_no_config',
        'pipelineName': 'no_config_pipeline',
        'runConfigOrError': {
        },
        'solidSelection': None,
        'tagsOrError': {
        }
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_out_of_process_env] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_multi_location] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinition',
        'mode': 'default',
        'name': 'triggered_no_config',
        'pipelineName': 'no_config_pipeline',
        'runConfigOrError': {
        },
        'solidSelection': None,
        'tagsOrError': {
        }
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_multi_location] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_managed_grpc_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinition',
        'mode': 'default',
        'name': 'triggered_no_config',
        'pipelineName': 'no_config_pipeline',
        'runConfigOrError': {
        },
        'solidSelection': None,
        'tagsOrError': {
        }
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_managed_grpc_env] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_in_process_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinition',
        'mode': 'default',
        'name': 'triggered_no_config',
        'pipelineName': 'no_config_pipeline',
        'runConfigOrError': {
            'yaml': '''storage:
  filesystem: {}
'''
        },
        'solidSelection': None,
        'tagsOrError': {
        }
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_in_process_env] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_out_of_process_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinition',
        'mode': 'default',
        'name': 'triggered_no_config',
        'pipelineName': 'no_config_pipeline',
        'runConfigOrError': {
            'yaml': '''storage:
  filesystem: {}
'''
        },
        'solidSelection': None,
        'tagsOrError': {
        }
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_out_of_process_env] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_multi_location] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinition',
        'mode': 'default',
        'name': 'triggered_no_config',
        'pipelineName': 'no_config_pipeline',
        'runConfigOrError': {
            'yaml': '''storage:
  filesystem: {}
'''
        },
        'solidSelection': None,
        'tagsOrError': {
        }
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_multi_location] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_managed_grpc_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinition',
        'mode': 'default',
        'name': 'triggered_no_config',
        'pipelineName': 'no_config_pipeline',
        'runConfigOrError': {
            'yaml': '''storage:
  filesystem: {}
'''
        },
        'solidSelection': None,
        'tagsOrError': {
        }
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_managed_grpc_env] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_deployed_grpc_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinition',
        'mode': 'default',
        'name': 'triggered_no_config',
        'pipelineName': 'no_config_pipeline',
        'runConfigOrError': {
            'yaml': '''storage:
  filesystem: {}
'''
        },
        'solidSelection': None,
        'tagsOrError': {
        }
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_deployed_grpc_env] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_executions[readonly_in_memory_instance_in_process_env] 1'] = {
    'triggerDefinitionsOrError': {
        '__typename': 'TriggerDefinitions',
        'results': [
            {
                'mode': 'default',
                'name': 'triggered_no_config',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            }
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_executions[readonly_in_memory_instance_out_of_process_env] 1'] = {
    'triggerDefinitionsOrError': {
        '__typename': 'TriggerDefinitions',
        'results': [
            {
                'mode': 'default',
                'name': 'triggered_no_config',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            }
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_executions[readonly_in_memory_instance_multi_location] 1'] = {
    'triggerDefinitionsOrError': {
        '__typename': 'TriggerDefinitions',
        'results': [
            {
                'mode': 'default',
                'name': 'triggered_no_config',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            }
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_executions[readonly_in_memory_instance_managed_grpc_env] 1'] = {
    'triggerDefinitionsOrError': {
        '__typename': 'TriggerDefinitions',
        'results': [
            {
                'mode': 'default',
                'name': 'triggered_no_config',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            }
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_executions[readonly_sqlite_instance_in_process_env] 1'] = {
    'triggerDefinitionsOrError': {
        '__typename': 'TriggerDefinitions',
        'results': [
            {
                'mode': 'default',
                'name': 'triggered_no_config',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            }
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_executions[readonly_sqlite_instance_out_of_process_env] 1'] = {
    'triggerDefinitionsOrError': {
        '__typename': 'TriggerDefinitions',
        'results': [
            {
                'mode': 'default',
                'name': 'triggered_no_config',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            }
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_executions[readonly_sqlite_instance_multi_location] 1'] = {
    'triggerDefinitionsOrError': {
        '__typename': 'TriggerDefinitions',
        'results': [
            {
                'mode': 'default',
                'name': 'triggered_no_config',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            }
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_executions[readonly_sqlite_instance_managed_grpc_env] 1'] = {
    'triggerDefinitionsOrError': {
        '__typename': 'TriggerDefinitions',
        'results': [
            {
                'mode': 'default',
                'name': 'triggered_no_config',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            }
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_executions[readonly_sqlite_instance_deployed_grpc_env] 1'] = {
    'triggerDefinitionsOrError': {
        '__typename': 'TriggerDefinitions',
        'results': [
            {
                'mode': 'default',
                'name': 'triggered_no_config',
                'pipelineName': 'no_config_pipeline',
                'solidSelection': None
            }
        ]
    }
}
