import click

from streamxfer import StreamXfer
from streamxfer import compress
from streamxfer import format
from streamxfer.cmd import Compress
from streamxfer.cmd import S3Sink, LocalSink
from streamxfer.format import Format


@click.command()
@click.argument("url")
@click.argument("table")
@click.argument("path")
@click.option(
    "-F",
    "--format",
    default=Format.CSV,
    choices=format.supported,
    case_sensitive=False,
    show_default=True,
)
@click.option(
    "--compress-type",
    default=Compress.lzop.name,
    choices=compress.supported,
    case_sensitive=False,
    show_default=True,
)
def cli(url, table, path, format, compress_type):
    sx = StreamXfer(
        url, format.upper(), enable_compress=True, compress_type=compress_type.upper()
    )
    if path.startswith("s3://"):
        sink = S3Sink
    else:
        sink = LocalSink
    sx.build(table, path, sink)
    sx.pump()


if __name__ == "__main__":
    cli()
