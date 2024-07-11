import click

from streamxfer import StreamXfer
from streamxfer.compress import supported as supported_compress
from streamxfer.format import supported as supported_format
from streamxfer.format import Format


@click.command()
@click.argument("pymssql-url")
@click.argument("table")
@click.argument("output-path")
@click.option(
    "-F",
    "--format",
    default=Format.JSON,
    type=click.Choice(supported_format, case_sensitive=False),
    show_default=True,
)
@click.option(
    "-C",
    "--compress-type",
    default=None,
    type=click.Choice(supported_compress, case_sensitive=False),
    show_default=True,
)
def cli(
    pymssql_url,
    table,
    output_path,
    format,
    compress_type,
):
    """StreamXfer is a powerful tool for streaming data from SQL Server to local or object storage(S3) for seamless transfer
    using UNIX pipe, supporting various general data formats(CSV, TSV, JSON).

    \b
    Examples:
        stx 'mssql+pymssql:://user:pass@host:port/db' '[dbo].[test]' /local/path/to/dir/
        stx 'mssql+pymssql:://user:pass@host:port/db' '[dbo].[test]' s3://bucket/path/to/dir/

    """
    sx = StreamXfer(
        pymssql_url,
        format,
        compress_type=compress_type,
    )
    sx.build(table, output_path)
    sx.pump()


if __name__ == "__main__":
    cli()
