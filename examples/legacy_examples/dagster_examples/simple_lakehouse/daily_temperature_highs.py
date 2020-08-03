import pandas as pd
from pandas import DataFrame as PandasDF
from lakehouse import Column, computed_table
from pyarrow import float64, date32
from .sfo_q2_weather_sample import sfo_q2_weather_sample_table


@computed_table(
    storage_key='filesystem',
    path=(__name__.split('.')[-1],),
    input_assets=[sfo_q2_weather_sample_table],
    columns=[Column('valid_date', date32()), Column('max_tmpf', float64())],
)
def daily_temperature_highs_table(sfo_q2_weather_sample: PandasDF) -> PandasDF:
    '''Computes the temperature high for each day'''
    sfo_q2_weather_sample['valid_date'] = pd.to_datetime(sfo_q2_weather_sample['valid'])
    return sfo_q2_weather_sample.groupby('valid_date').max().rename(columns={'tmpf': 'max_tmpf'})
