import sys

import click

from streamxfer.format import read_stream
from streamxfer.mssql import mssql_csv_escape, csv_bin_rtx


@click.command()
@click.argument(
    "input", type=click.File(mode="rb", encoding="utf-8"), default=sys.stdin.buffer
)
@click.argument(
    "output", type=click.File(mode="w", encoding="utf-8"), default=sys.stdout
)
def cli(input, output):
    for line in read_stream(input, newline=csv_bin_rtx):
        ln = mssql_csv_escape(line)
        output.write(ln)


if __name__ == "__main__":
    cli()
