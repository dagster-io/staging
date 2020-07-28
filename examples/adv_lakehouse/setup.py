from setuptools import setup

setup(
    name='adv_lakehouse',
    version='dev',
    description='Dagster Examples',
    author='Elementl',
    author_email='chris@elementl.com',
    packages=['adv_lakehouse'],  # same as name
    license='Apache-2.0',
    url='https://github.com/dagster-io/dagster/tree/master/examples/adv_lakehouse',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
)
