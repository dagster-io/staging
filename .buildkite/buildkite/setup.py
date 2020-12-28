from setuptools import find_packages, setup

setup(
    name="buildkite",
    version="0.0.1",
    author="Elementl",
    author_email="hello@elementl.com",
    license="Apache-2.0",
    description="Tools for buildkite automation",
    url="https://github.com/dagster-io/dagster/tree/master/.buildkite/buildkite",
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(exclude=["test"]),
    install_requires=["PyYAML"],
    entry_points={
        "console_scripts": [
            "buildkite-dagster = buildkite.cli:dagster",
            "buildkite-integration = buildkite.cli:integration",
        ]
    },
)
