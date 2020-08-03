from lakehouse import source_table, Column
from pyarrow import float64, string


sfo_q2_weather_sample_table = source_table(
    storage_key='filesystem',
    path=('sfo_q2_weather_sample',),
    columns=[Column('tmpf', float64()), Column('valid_date', string())],
)
