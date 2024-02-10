import io

from setuptools import setup, find_packages

__version__ = "0.0.1"

with open("requirements.txt", encoding="utf-8") as requirements_file:
    requirements = [_.strip() for _ in requirements_file.readlines()]

setup(
    name="streamxfer",
    version=__version__,
    description="StreamXfer is a powerful tool for streaming data from SQL Server to object storage for seamless transfer using UNIX pipe, supporting various general data formats(CSV, TSV, JSON).",
    long_description=io.open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    python_requires=">=3.9",
    entry_points={
        "console_scripts": [
            "stx = streamxfer.cli.main:cli",
            "stx-mssql-csv-escape = streamxfer.cli.mssql_csv_escape:cli",
            "stx-mssql-json-escape = streamxfer.cli.mssql_json_escape:cli",
            "stx-redshift-escape = streamxfer.cli.redshift_escape:cli",
        ],
    },
)
