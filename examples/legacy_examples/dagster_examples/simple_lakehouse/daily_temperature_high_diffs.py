from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import Window
from pyspark.sql import functions as f

from lakehouse import Column, computed_table
from pyarrow import float64, date32
from dagster_examples.simple_lakehouse.daily_temperature_highs import daily_temperature_highs_table


@computed_table(
    storage_key='filesystem',
    path=(__name__.split('.')[-1],),
    input_assets=[daily_temperature_highs_table],
    columns=[Column('valid_date', date32()), Column('max_tmpf', float64())],
)
def daily_temperature_high_diffs_table(daily_temperature_highs: SparkDF) -> SparkDF:
    '''Computes the difference between each day's high and the previous day's high'''
    window = Window.orderBy('valid_date')
    return daily_temperature_highs.select(
        'valid_date',
        (
            daily_temperature_highs['max_tmpf']
            - f.lag(daily_temperature_highs['max_tmpf']).over(window)
        ).alias('day_high_diff'),
    )
