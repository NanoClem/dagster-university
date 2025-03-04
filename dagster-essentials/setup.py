from setuptools import find_packages, setup

setup(
    name="dagster_essentials",
    packages=find_packages(exclude=["dagster_essentials_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-duckdb",
        "pandas>=2.2.3",
        "geopandas>=1.0.1",
        "matplotlib>=3.9.4",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
