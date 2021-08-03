from dagster_airflow.factory import make_airflow_dag_containerized

dag, steps = make_airflow_dag_containerized(
    module_name="docs_snippets.deploying.airflow.pipeline",
    pipeline_name="hello_cereal_pipeline",
    image="dagster-airflow-demo-repository",
    run_config={"resources": {"io_manager": {"config": {"base_dir": "/container_tmp"}}}},
    dag_id=None,
    dag_description=None,
    dag_kwargs=None,
    op_kwargs=None,
)
