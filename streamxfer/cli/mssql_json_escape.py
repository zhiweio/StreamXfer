import sys

import click

from streamxfer.mssql import mssql_json_escape


@click.command()
@click.argument("input", type=click.File(mode="r", encoding="utf-8"), default=sys.stdin)
@click.argument(
    "output", type=click.File(mode="w", encoding="utf-8"), default=sys.stdout
)
def cli(input, output):
    for line in input:
        ln = mssql_json_escape(line)
        output.write(ln)


if __name__ == "__main__":
    cli()
