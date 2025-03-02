from setuptools import find_packages, setup

setup(
    name="dagster_essentials",
    packages=find_packages(exclude=["dagster_essentials_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-duckdb",
        "pandas>=2.2.3",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
