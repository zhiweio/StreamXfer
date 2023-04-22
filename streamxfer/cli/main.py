import click

from streamxfer import StreamXfer
from streamxfer import compress
from streamxfer import format
from streamxfer.cmd import Compress
from streamxfer.cmd import S3Sink, LocalSink
from streamxfer.format import Format


@click.command()
@click.argument("pymssql-url")
@click.argument("table")
@click.argument("output-path")
@click.option(
    "-F",
    "--format",
    default=Format.JSON,
    type=click.Choice(format.supported, case_sensitive=False),
    show_default=True,
)
@click.option(
    "--compress-type",
    default=Compress.lzop.name,
    type=click.Choice(compress.supported, case_sensitive=False),
    show_default=True,
)
@click.option("--no-compress", "disable_compress", is_flag=True)
@click.option("--redshift-escape", "enable_redshift_escape", is_flag=True)
def cli(
    pymssql_url,
    table,
    output_path,
    format,
    compress_type,
    disable_compress,
    enable_redshift_escape,
):
    """StreamXfer is a powerful tool for streaming data from SQL Server to object storage for seamless transfer
    using UNIX pipe, supporting various general data formats(CSV, TSV, JSON).

    \b
    Examples:
        stx 'mssql+pymssql:://user:pass@host:port/db' '[dbo].[test]' /local/path/to/dir/
        stx 'mssql+pymssql:://user:pass@host:port/db' '[dbo].[test]' s3://bucket/path/to/dir/

    """
    sx = StreamXfer(
        pymssql_url,
        format,
        enable_compress=not disable_compress,
        compress_type=compress_type,
    )
    if output_path.startswith("s3://"):
        sink = S3Sink()
    else:
        sink = LocalSink()
    sx.build(table, output_path, sink, enable_redshift_escape)
    sx.pump()


if __name__ == "__main__":
    cli()
