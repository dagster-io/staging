from adv_lakehouse.pipelines import adv_lakehouse_pipeline

from dagster import execute_pipeline


def test_adv_lakehouse():
    pipeline_result = execute_pipeline(adv_lakehouse_pipeline, mode='dev')
    assert pipeline_result.success
