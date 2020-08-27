# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_in_process_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'PythonError',
        'message': '''dagster.check.ParameterCheckError: Param "external_triggered_execution" is not a ExternalTriggeredExecution. Got <dagster_graphql.schema.triggers.DauphinTriggerDefinition object at 0x11076a710> which is type TriggerDefinition.
''',
        'stack': [
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/utils.py", line 14, in _fn
    return fn(*args, **kwargs)
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/fetch_triggers.py", line 47, in get_trigger_definition_or_error
    return graphene_info.schema.type_named("TriggerDefinition")(results[0])
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/schema/triggers.py", line 64, in __init__
    external_triggered_execution, "external_triggered_execution", ExternalTriggeredExecution
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster/dagster/check/__init__.py", line 183, in inst_param
    obj, ttype, param_name, additional_message=additional_message
''',
            '''  File "/Users/prha/.pyenv/versions/3.6.8/envs/dagster-3.6.8/lib/python3.6/site-packages/future/utils/__init__.py", line 446, in raise_with_traceback
    raise exc.with_traceback(traceback)
'''
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_in_process_env] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_out_of_process_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'PythonError',
        'message': '''dagster.check.ParameterCheckError: Param "external_triggered_execution" is not a ExternalTriggeredExecution. Got <dagster_graphql.schema.triggers.DauphinTriggerDefinition object at 0x110b62e80> which is type TriggerDefinition.
''',
        'stack': [
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/utils.py", line 14, in _fn
    return fn(*args, **kwargs)
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/fetch_triggers.py", line 47, in get_trigger_definition_or_error
    return graphene_info.schema.type_named("TriggerDefinition")(results[0])
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/schema/triggers.py", line 64, in __init__
    external_triggered_execution, "external_triggered_execution", ExternalTriggeredExecution
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster/dagster/check/__init__.py", line 183, in inst_param
    obj, ttype, param_name, additional_message=additional_message
''',
            '''  File "/Users/prha/.pyenv/versions/3.6.8/envs/dagster-3.6.8/lib/python3.6/site-packages/future/utils/__init__.py", line 446, in raise_with_traceback
    raise exc.with_traceback(traceback)
'''
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_out_of_process_env] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_multi_location] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'PythonError',
        'message': '''dagster.check.ParameterCheckError: Param "external_triggered_execution" is not a ExternalTriggeredExecution. Got <dagster_graphql.schema.triggers.DauphinTriggerDefinition object at 0x111127b70> which is type TriggerDefinition.
''',
        'stack': [
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/utils.py", line 14, in _fn
    return fn(*args, **kwargs)
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/fetch_triggers.py", line 47, in get_trigger_definition_or_error
    return graphene_info.schema.type_named("TriggerDefinition")(results[0])
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/schema/triggers.py", line 64, in __init__
    external_triggered_execution, "external_triggered_execution", ExternalTriggeredExecution
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster/dagster/check/__init__.py", line 183, in inst_param
    obj, ttype, param_name, additional_message=additional_message
''',
            '''  File "/Users/prha/.pyenv/versions/3.6.8/envs/dagster-3.6.8/lib/python3.6/site-packages/future/utils/__init__.py", line 446, in raise_with_traceback
    raise exc.with_traceback(traceback)
'''
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_multi_location] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_managed_grpc_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'PythonError',
        'message': '''dagster.check.ParameterCheckError: Param "external_triggered_execution" is not a ExternalTriggeredExecution. Got <dagster_graphql.schema.triggers.DauphinTriggerDefinition object at 0x11065c9b0> which is type TriggerDefinition.
''',
        'stack': [
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/utils.py", line 14, in _fn
    return fn(*args, **kwargs)
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/fetch_triggers.py", line 47, in get_trigger_definition_or_error
    return graphene_info.schema.type_named("TriggerDefinition")(results[0])
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/schema/triggers.py", line 64, in __init__
    external_triggered_execution, "external_triggered_execution", ExternalTriggeredExecution
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster/dagster/check/__init__.py", line 183, in inst_param
    obj, ttype, param_name, additional_message=additional_message
''',
            '''  File "/Users/prha/.pyenv/versions/3.6.8/envs/dagster-3.6.8/lib/python3.6/site-packages/future/utils/__init__.py", line 446, in raise_with_traceback
    raise exc.with_traceback(traceback)
'''
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_in_memory_instance_managed_grpc_env] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_in_process_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'PythonError',
        'message': '''dagster.check.ParameterCheckError: Param "external_triggered_execution" is not a ExternalTriggeredExecution. Got <dagster_graphql.schema.triggers.DauphinTriggerDefinition object at 0x11065ab70> which is type TriggerDefinition.
''',
        'stack': [
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/utils.py", line 14, in _fn
    return fn(*args, **kwargs)
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/fetch_triggers.py", line 47, in get_trigger_definition_or_error
    return graphene_info.schema.type_named("TriggerDefinition")(results[0])
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/schema/triggers.py", line 64, in __init__
    external_triggered_execution, "external_triggered_execution", ExternalTriggeredExecution
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster/dagster/check/__init__.py", line 183, in inst_param
    obj, ttype, param_name, additional_message=additional_message
''',
            '''  File "/Users/prha/.pyenv/versions/3.6.8/envs/dagster-3.6.8/lib/python3.6/site-packages/future/utils/__init__.py", line 446, in raise_with_traceback
    raise exc.with_traceback(traceback)
'''
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_in_process_env] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_out_of_process_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'PythonError',
        'message': '''dagster.check.ParameterCheckError: Param "external_triggered_execution" is not a ExternalTriggeredExecution. Got <dagster_graphql.schema.triggers.DauphinTriggerDefinition object at 0x110a080f0> which is type TriggerDefinition.
''',
        'stack': [
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/utils.py", line 14, in _fn
    return fn(*args, **kwargs)
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/fetch_triggers.py", line 47, in get_trigger_definition_or_error
    return graphene_info.schema.type_named("TriggerDefinition")(results[0])
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/schema/triggers.py", line 64, in __init__
    external_triggered_execution, "external_triggered_execution", ExternalTriggeredExecution
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster/dagster/check/__init__.py", line 183, in inst_param
    obj, ttype, param_name, additional_message=additional_message
''',
            '''  File "/Users/prha/.pyenv/versions/3.6.8/envs/dagster-3.6.8/lib/python3.6/site-packages/future/utils/__init__.py", line 446, in raise_with_traceback
    raise exc.with_traceback(traceback)
'''
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_out_of_process_env] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_multi_location] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'PythonError',
        'message': '''dagster.check.ParameterCheckError: Param "external_triggered_execution" is not a ExternalTriggeredExecution. Got <dagster_graphql.schema.triggers.DauphinTriggerDefinition object at 0x110bd60f0> which is type TriggerDefinition.
''',
        'stack': [
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/utils.py", line 14, in _fn
    return fn(*args, **kwargs)
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/fetch_triggers.py", line 47, in get_trigger_definition_or_error
    return graphene_info.schema.type_named("TriggerDefinition")(results[0])
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/schema/triggers.py", line 64, in __init__
    external_triggered_execution, "external_triggered_execution", ExternalTriggeredExecution
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster/dagster/check/__init__.py", line 183, in inst_param
    obj, ttype, param_name, additional_message=additional_message
''',
            '''  File "/Users/prha/.pyenv/versions/3.6.8/envs/dagster-3.6.8/lib/python3.6/site-packages/future/utils/__init__.py", line 446, in raise_with_traceback
    raise exc.with_traceback(traceback)
'''
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_multi_location] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_managed_grpc_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'PythonError',
        'message': '''dagster.check.ParameterCheckError: Param "external_triggered_execution" is not a ExternalTriggeredExecution. Got <dagster_graphql.schema.triggers.DauphinTriggerDefinition object at 0x110ffbac8> which is type TriggerDefinition.
''',
        'stack': [
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/utils.py", line 14, in _fn
    return fn(*args, **kwargs)
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/fetch_triggers.py", line 47, in get_trigger_definition_or_error
    return graphene_info.schema.type_named("TriggerDefinition")(results[0])
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/schema/triggers.py", line 64, in __init__
    external_triggered_execution, "external_triggered_execution", ExternalTriggeredExecution
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster/dagster/check/__init__.py", line 183, in inst_param
    obj, ttype, param_name, additional_message=additional_message
''',
            '''  File "/Users/prha/.pyenv/versions/3.6.8/envs/dagster-3.6.8/lib/python3.6/site-packages/future/utils/__init__.py", line 446, in raise_with_traceback
    raise exc.with_traceback(traceback)
'''
        ]
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_managed_grpc_env] 2'] = {
    'triggerDefinitionOrError': {
        '__typename': 'TriggerDefinitionNotFoundError'
    }
}

snapshots['TestTriggeredExecutions.test_get_triggered_execution[readonly_sqlite_instance_deployed_grpc_env] 1'] = {
    'triggerDefinitionOrError': {
        '__typename': 'PythonError',
        'message': '''dagster.check.ParameterCheckError: Param "external_triggered_execution" is not a ExternalTriggeredExecution. Got <dagster_graphql.schema.triggers.DauphinTriggerDefinition object at 0x11079df60> which is type TriggerDefinition.
''',
        'stack': [
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/utils.py", line 14, in _fn
    return fn(*args, **kwargs)
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/implementation/fetch_triggers.py", line 47, in get_trigger_definition_or_error
    return graphene_info.schema.type_named("TriggerDefinition")(results[0])
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql/schema/triggers.py", line 64, in __init__
    external_triggered_execution, "external_triggered_execution", ExternalTriggeredExecution
''',
            '''  File "/Users/prha/code/dagster/python_modules/dagster/dagster/check/__init__.py", line 183, in inst_param
    obj, ttype, param_name, additional_message=additional_message
''',
            '''  File "/Users/prha/.pyenv/versions/3.6.8/envs/dagster-3.6.8/lib/python3.6/site-packages/future/utils/__init__.py", line 446, in raise_with_traceback
    raise exc.with_traceback(traceback)
'''
        ]
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
