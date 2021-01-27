from dagster import pipeline
from ..solids import add, constant_one


@pipeline
def add_ones():
    add(contant_one(), constant_one())
