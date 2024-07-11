import sys
import click
from streamxfer.mssql import (
    mssql_tsv_escape,
    mssql_json_escape,
    mssql_csv_escape,
    csv_bin_rtx,
)
from streamxfer.format import read_stream


@click.group()
def cli():
    """Convert input to supported formats (TSV, CSV, JSON) with MSSQL escaping."""


@cli.command("tsv", short_help="escape tsv data")
@click.argument(
    "input",
    type=click.File(mode="r", encoding="utf-8"),
    default=sys.stdin,
)
@click.argument(
    "output",
    type=click.File(mode="w", encoding="utf-8"),
    default=sys.stdout,
)
def tsv(input, output):
    for line in input:
        ln = mssql_tsv_escape(line)
        output.write(ln)


@cli.command("json", short_help="escape json data")
@click.argument(
    "input",
    type=click.File(mode="r", encoding="utf-8"),
    default=sys.stdin,
)
@click.argument(
    "output",
    type=click.File(mode="w", encoding="utf-8"),
    default=sys.stdout,
)
def json(input, output):
    for line in input:
        ln = mssql_json_escape(line)
        output.write(ln)


@cli.command("csv", short_help="escape csv data")
@click.argument(
    "input",
    type=click.File(mode="rb", encoding="utf-8"),
    default=sys.stdin.buffer,
)
@click.argument(
    "output",
    type=click.File(mode="w", encoding="utf-8"),
    default=sys.stdout,
)
def csv(input, output):
    for line in read_stream(input, newline=csv_bin_rtx):
        ln = mssql_csv_escape(line)
        output.write(ln)


if __name__ == "__main__":
    cli()
