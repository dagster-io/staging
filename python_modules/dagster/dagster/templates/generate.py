import click
import os
import shutil

PATH_SIMPLE = os.path.join(os.path.dirname(__file__), "simple")


def generate_simple_project(path: str):
    # Copy files into project.
    print(PATH_SIMPLE)
    shutil.copytree(PATH_SIMPLE, path)
