# from dagster import executor
# from dagster.core.executor.extendable.core_executor import CoreExecutor
# from dagster.core.executor.extendable.step_handler import StepHandler


# class DagsterK8sStepHandler(StepHandler):
#     pass


# @executor(
#     name="dagster-k8s",
#     config_schema={},
# )
# def dagster_k8s_executor(_init_context):

#     return CoreExecutor(DagsterK8sStepHandler())
