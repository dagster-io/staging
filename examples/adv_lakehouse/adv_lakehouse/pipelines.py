'''Pipeline definitions for the adv_lakehouse example.
'''
from adv_lakehouse.assets import daily_temperature_high_diffs_table, daily_temperature_highs_table
from adv_lakehouse.lakehouse_def import adv_lakehouse

computed_assets = [daily_temperature_highs_table, daily_temperature_high_diffs_table]
adv_lakehouse_pipeline = adv_lakehouse.build_pipeline_definition(
    'adv_lakehouse_pipeline', computed_assets,
)
