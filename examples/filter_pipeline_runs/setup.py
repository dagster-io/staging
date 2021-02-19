import setuptools

setuptools.setup(
    name="filter_pipeline_runs",
    version="dev",
    author_email="hello@elementl.com",
    packages=["filter_pipeline_runs"],
    include_package_data=True,
    install_requires=["dagster"],
    author="Elementl",
    license="Apache-2.0",
    description="Dagster example for get a list of pipeline runs of a certain status.",
    url="https://github.com/dagster-io/dagster/tree/master/examples/filter_pipeline_runs",
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
)
