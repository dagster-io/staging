from adv_lakehouse.pipelines import adv_lakehouse_pipeline

from dagster import repository


@repository
def adv_lakehouse():
    return [adv_lakehouse_pipeline]
