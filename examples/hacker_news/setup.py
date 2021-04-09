import setuptools

setuptools.setup(
    name="hacker_news",
    version="dev",
    author="Elementl",
    author_email="hello@elementl.com",
    license="Apache-2.0",
    packages=setuptools.find_packages(exclude=["hacker_news_tests"]),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "dagster",
        "dagster-gcp",
        "google-cloud-bigquery[bqstorage,pandas]",
        "mock",
        "pandas",
        "pyarrow",
        "gcsfs",
        "fsspec",
        "scipy",
        "sklearn",
        "snowflake-sqlalchemy",
    ],
)
