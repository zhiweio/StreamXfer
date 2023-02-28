import textwrap
from typing import Union, List, Dict

import sqlalchemy
from sqlalchemy import create_engine, make_url

from streamxfer import mssql as ms
from streamxfer.compress import supported, COMPRESS_LEVEL
from streamxfer.format import Format, sc
from streamxfer.utils import quote_this, IS_MACOS


class Cat:
    name = "cat"
    bin = "gcat" if IS_MACOS else "cat"

    @classmethod
    def cmd(cls, file=None, shell=True) -> Union[List[str], str]:
        _cmd = [cls.bin]
        if file is not None:
            _cmd.append(quote_this(file))
        if shell:
            return " ".join(_cmd)
        return _cmd


class _Compress:
    name = None
    bin = None
    ext = None

    @classmethod
    def cmd(cls, level=COMPRESS_LEVEL, shell=True) -> Union[List[str], str]:
        _cmd = [cls.bin, f"-{level}"]
        if shell:
            return " ".join(_cmd)
        return _cmd


class GZIP(_Compress):
    name = "GZIP"
    bin = "gzip"
    ext = ".gz"


class LZOP(_Compress):
    name = "LZOP"
    bin = "lzop"
    ext = ".lzo"


class Compress:
    lzop = LZOP
    gzip = GZIP

    def __init__(self, type):
        assert type in supported, f"compress {type} does not support"
        self.type = type

    def cmd(self, level=COMPRESS_LEVEL, shell=True) -> Union[List[str], str]:
        return getattr(self, self.type.lower()).cmd(level, shell)

    def ext(self):
        return getattr(self, self.type.lower()).ext


class MssqlCsvEscape:
    name = "stx-mssql-csv-escape"
    bin = "stx-mssql-csv-escape"

    @classmethod
    def cmd(cls, shell=True) -> Union[List[str], str]:
        return cls.bin


class RedshiftEscape:
    name = "stx-redshift-escape"
    bin = "stx-redshift-escape"

    @classmethod
    def cmd(cls, shell=True) -> Union[List[str], str]:
        return cls.bin


class Split:
    name = "split"
    bin = "gsplit" if IS_MACOS else "split"

    @classmethod
    def cmd(cls, filter: str, lines=1000000, shell=True) -> Union[List[str], str]:
        _cmd = [
            cls.bin,
            "-l",
            str(lines),
            "--numeric-suffixes",
            "--suffix-length=8",
            "--filter",
            quote_this(filter),
        ]
        if shell:
            return " ".join(_cmd)
        return _cmd


class BCP:
    name = "bcp"
    bin = "bcp"

    @classmethod
    def cmd(
        cls,
        table: str,
        pymssql_url,
        flat_file: str,
        direc="queryout",
        format: str = Format.TSV,
        field_terminator=sc.TAB,
        row_terminator=sc.LN,
        packet_size: int = 65535,
        shell=True,
        conn: sqlalchemy.Connection = None,
    ) -> Union[List[str], str]:
        if conn is None:
            engine = create_engine("mssql+pymssql://scott:tiger@hostname:port/dbname")
            with engine.connect() as conn:
                query = _build_bcp_query(table, format, conn)
        else:
            query = _build_bcp_query(table, format, conn)
        query = quote_this("".join(query.splitlines()))

        url = make_url(pymssql_url)
        auth = ["-U", url.username, "-P", url.password]
        _cmd = [
            cls.bin,
            query,
            direc,
            quote_this(flat_file),
            "-S",
            url.host,
            "-d",
            url.database,
            "-q",  # Executes the SET QUOTED_IDENTIFIERS ON statement, needed for Azure SQL DW,
            "-c",
            "-C",
            "65001",
            "-a",
            str(packet_size),
        ] + auth
        if format in [Format.TSV, Format.CSV]:
            _cmd += [
                "-t",
                f"'{field_terminator}'",
                "-r",
                f"'{row_terminator}'",
            ]
        if shell:
            return " ".join(_cmd)
        return _cmd


def _build_bcp_query(table, format: str, conn: sqlalchemy.Connection):
    columns = ms.table_columns(table, conn)
    if format == Format.JSON:
        expr = _concat_columns(columns)
        query = textwrap.dedent(
            f"""
            SELECT(SELECT {expr}
                   FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER)
            FROM {table} (nolock)
        """
        )
    elif format == Format.TSV:
        expr = _concat_columns(columns, json_string_escape=True)
        query = f"SELECT {expr} FROM {table} (nolock)"
    else:
        expr = _concat_columns(columns)
        query = f"SELECT {expr} FROM {table} (nolock)"
    return query


def _concat_columns(columns: List[Dict[str, str]], json_string_escape=False) -> str:
    exps = []
    for c in columns:
        name = "[" + c["column_name"] + "]"
        type = c["column_type"]
        if json_string_escape and type in ms.Keywords.string_types:
            if type in (ms.Keywords.NTEXT, ms.Keywords.TEXT):
                name = f"CONVERT(NVARCHAR(MAX), {name})"
            exp = f"STRING_ESCAPE({name}, 'json') AS {name}"
        else:
            exp = name
        exps.append(exp)
    return ", ".join(exps)


class LocalSink:
    bin = Cat.bin

    def cmd(self, uri) -> Union[List[str], str]:
        _cmd = [self.bin, ">", uri]
        return " ".join(_cmd)


class S3Sink:
    bin = "aws s3 cp"

    def cmd(self, uri) -> Union[List[str], str]:
        _cmd = [self.bin, "-", uri]
        return " ".join(_cmd)
