from setuptools import find_packages, setup

setup(
    name="pdga",
    packages=find_packages(exclude=["pdga_tests"]),
    install_requires=[
        "dagster",
        "pandas",
        "snowflake.connector",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
