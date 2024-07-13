# StreamXfer

StreamXfer is a powerful tool for streaming data from SQL Server to local or object storage(S3) for seamless transfer using UNIX
pipe, supporting various general data formats(CSV, TSV, JSON).

**Supported OS:** Linux, macOS

_I've migrated 10TB data from SQL Server into Amazon Redshift using this tool._

## Demo

[![asciicast](https://asciinema.org/a/563200.svg)](https://asciinema.org/a/563200)


## Installation

**Prerequisites**

Before installing StreamXfer, you need to install the following dependencies:

* mssql-tools: [SQL Docs - bcp Utility](https://learn.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver16)
* lzop: [Download](https://www.lzop.org/)
* awscli: [AWS CLI install and update instructions](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html#getting-started-install-instructions)

**Install from PyPI**

```shell
$ python3 -m pip install streamxfer
```

**Install from Source**

```shell
$ git clone https://github.com/zhiweio/StreamXfer.git && cd StreamXfer/
$ python3 setup.py install
```

**Install from Docker**

```shell
$ docker pull zhiweio/streamxfer:latest
```

## Usage

StreamXfer can be used as a command-line tool or as a library in Python.

### Command-line Usage

```shell
$ stx [OPTIONS] PYMSSQL_URL TABLE OUTPUT_PATH
```

Here is an example command:

```shell
$ stx 'mssql+pymssql:://user:pass@host:port/db' '[dbo].[test]' /local/path/to/dir/
```

You can also use the following options:

* `-F, --format`: The data format (CSV, TSV, or JSON).
* `-C, --compress-type`: The compression type (LZOP or GZIP).

For more detailed options, run:

```shell
$ stx --help
Usage: stx [OPTIONS] PYMSSQL_URL TABLE OUTPUT_PATH

  StreamXfer is a powerful tool for streaming data from SQL Server to local or
  object storage(S3) for seamless transfer using UNIX pipe, supporting various
  general data formats(CSV, TSV, JSON).

  Examples:
      stx 'mssql+pymssql:://user:pass@host:port/db' '[dbo].[test]' /local/path/to/dir/
      stx 'mssql+pymssql:://user:pass@host:port/db' '[dbo].[test]' s3://bucket/path/to/dir/

Options:
  -F, --format [CSV|TSV|JSON]     [default: JSON]
  -C, --compress-type [LZOP|GZIP]
  -h, --help                      Show this message and exit.
```

### Docker Usage

To use StreamXfer in Docker container:

```shell
$ docker run --rm -v $(pwd)/data:/tmp/data zhiweio/streamxfer bash -c "stx 'mssql+pymssql:://user:pass@host:port/db' '[dbo].[test]' /tmp/data/dbo_test"
$ docker run --rm zhiweio/streamxfer bash -c "stx 'mssql+pymssql:://user:pass@host:port/db' '[dbo].[test]' s3://bucket/path/to/dir"
```

### Library Usage

To use StreamXfer as a library in Python, you can import the StreamXfer class, and use them to build and pump the data stream.

Here is an example code snippet:

```python
from streamxfer import StreamXfer
from streamxfer.format import Format
from streamxfer.compress import CompressType

sx = StreamXfer(
    "mssql+pymssql:://user:pass@host:port/db",
    format=Format.CSV,
    compress_type=CompressType.LZOP,
    chunk_size=1000000,
)
sx.build("[dbo].[test]", path="s3://bucket/path/to/dir/")
sx.pump()

```

## Related

Here are some related articles

* [How to stream Microsoft SQL Server to S3 using BCP on linux](https://dstan.medium.com/streaming-microsoft-sql-server-to-s3-using-bcp-35241967d2e0)

## Authors

- [@zhiweio](https://www.github.com/zhiweio)

## License

[GPL-3.0](https://choosealicense.com/licenses/gpl-3.0/)
