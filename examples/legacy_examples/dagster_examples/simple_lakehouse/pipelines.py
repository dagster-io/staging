'''Pipeline definitions for the simple_lakehouse example.
'''
from dagster_examples.simple_lakehouse.assets import (
    daily_temperature_high_diffs_table,
    daily_temperature_highs_table,
)
from dagster_examples.simple_lakehouse.simple_lakehouse import simple_lakehouse

computed_assets = [daily_temperature_highs_table, daily_temperature_high_diffs_table]
simple_lakehouse_pipeline = simple_lakehouse.build_pipeline_definition(
    'simple_lakehouse_pipeline', computed_assets,
)
