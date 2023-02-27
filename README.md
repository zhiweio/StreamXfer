# StreamXfer

StreamXfer is a powerful tool for streaming data from SQL Server to object storage for seamless transfer using UNIX
pipe, supporting various general data formats(CSV, TSV, JSON).


## Installation

Before installing StreamXfer, you need to install the following dependencies:

* mssql-tools: [SQL Docs - bcp Utility](https://learn.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver16)
* lzop: `yum install lzop`

Then, install StreamXfer from source:

```shell
$ git clone https://github.com/zhiweio/StreamXfer.git && cd StreamXfer/
$ python3 setup.py install
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
* `--compress-type`: The compression type (LZOP or GZIP).
* `--no-compress`: Disables compression.

For more information on the options, run stx --help.

```shell
$ stx --help
Usage: stx [OPTIONS] PYMSSQL_URL TABLE OUTPUT_PATH

  StreamXfer is a powerful tool for streaming data from SQL Server to object
  storage for seamless transfer using UNIX pipe, supporting various general
  data formats(CSV, TSV, JSON).

  Examples:
      stx 'mssql+pymssql:://user:pass@host:port/db' '[dbo].[test]' /local/path/to/dir/
      stx 'mssql+pymssql:://user:pass@host:port/db' '[dbo].[test]' s3://bucket/path/to/dir/

Options:
  -F, --format [CSV|TSV|JSON]  [default: TSV]
  --compress-type [LZOP|GZIP]  [default: LZOP]
  --no-compress
  --redshift-escape
  --help                       Show this message and exit.


```

### Library Usage

To use StreamXfer as a library in Python, you can import the StreamXfer class and the sink classes (such as S3Sink), and use them to build and pump the data stream.

Here is an example code snippet:

```python
from streamxfer import StreamXfer
from streamxfer.cmd import S3Sink

sx = StreamXfer(
    "mssql+pymssql:://user:pass@host:port/db",
    format="TSV",
    enable_compress=True,
    compress_type="LZOP",
)
sx.build("[dbo].[test]", "s3://bucket/path/to/dir/", sink=S3Sink())
sx.pump()

```

## Related

Here are some related articles

* [How to stream Microsoft SQL Server to S3 using BCP on linux](https://dstan.medium.com/streaming-microsoft-sql-server-to-s3-using-bcp-35241967d2e0)

## Authors

- [@zhiweio](https://www.github.com/zhiweio)

## License

[GPL-3.0](https://choosealicense.com/licenses/gpl-3.0/)
