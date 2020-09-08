from dagster_celery.make_app import make_app_with_task_routes

from .executor import create_k8s_job_task
from .launcher import create_k8s_run_coordinator_job_task

app = make_app_with_task_routes(task_routes={})

# All tasks must be called here to be registered
execute_step_k8s_job = create_k8s_job_task(app)
execute_run_coordinator_k8s_job = create_k8s_run_coordinator_job_task(app)
