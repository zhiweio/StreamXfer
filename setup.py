from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as readme_file:
    readme = readme_file.read()

with open("requirements.txt", encoding="utf-8") as requirements_file:
    requirements = [_.strip() for _ in requirements_file.readlines()]

setup(
    name="stx",
    version="0.1.0",
    description="StreamXfer is a powerful tool for streaming data from SQL Server to object storage for seamless transfer using UNIX pipe, supporting various general data formats(CSV, TSV, JSON).",
    long_description=readme,
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    python_requires=">=3.9",
    entry_points={
        "console_scripts": [
            "stx = streamxfer.cli.main:cli",
            "stx-mssql-csv-escape = streamxfer.cli.mssql_csv_escape:cli",
            "stx-redshift-escape = streamxfer.cli.redshift_escape:cli",
        ],
    },
)
