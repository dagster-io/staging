from dagster import repository

from .domainA import my_repository_A
from .domainB import my_repository_B


@repository
def my_repository():
    return my_repository_A.get_all_pipelines() + my_repository_B.get_all_pipelines()
