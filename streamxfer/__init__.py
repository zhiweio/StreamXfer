import os
from subprocess import Popen

import psutil

from streamxfer import mssql as ms
from streamxfer.typing import *
from streamxfer.cmd import (
    BCP,
    Split,
    Cat,
    MssqlTsvEscape,
    MssqlCsvEscape,
    MssqlJsonEscape,
    LZOPCompress,
    GZIPCompress,
)
from streamxfer.sink import uri2sink
from streamxfer.compress import CompressType
from streamxfer.format import Format, sc
from streamxfer.log import LOG
from streamxfer.utils import (
    mktempfifo,
    rmtempfifo,
    cmd2pipe,
    wait_until_created,
    contains_dot,
)

__module__ = ["StreamXfer"]
__all__ = ["StreamXfer"]


class StreamXfer:
    def __init__(
        self,
        url,
        format: str,
        compress_type: str = None,
        compress_level=6,
        chunk_size=1000000,
    ):
        self.url = url
        self.format = format
        self.chunk_size = chunk_size
        self.sink = None
        self.columns = None
        self.compress = None
        self._bcp = None
        self._pipe = None
        self._fifo = None

        if compress_type == CompressType.LZOP:
            self.compress = LZOPCompress
        elif compress_type == CompressType.GZIP:
            self.compress = GZIPCompress
        else:
            compress_type = None
        self.enable_compress = compress_type is not None
        self.compress_level = compress_level

    @property
    def bcp(self):
        return self._bcp

    @property
    def pipe(self):
        return self._pipe

    def add_escape(self, cmds: List[str]):
        if self.format == Format.TSV:
            cmds.insert(1, MssqlTsvEscape.cmd())
        elif self.format == Format.CSV:
            cmds.insert(1, MssqlCsvEscape.cmd())
        elif self.format == Format.JSON:
            if contains_dot(self.columns):
                cmds.insert(1, MssqlJsonEscape.cmd())

    def build(
        self,
        table,
        path: str,
    ):
        file_ext = "." + self.format.lower()
        if self.enable_compress:
            file_ext = file_ext + self.compress.ext

        self.sink = uri2sink(path)
        self.sink.set_file_extension(file_ext)
        self._fifo = mktempfifo(suffix=file_ext)

        if self.format == Format.CSV:
            ft = ms.csv_in_ft
            rt = ms.csv_in_rt
        else:
            ft = sc.TAB
            rt = sc.LN

        engine = ms.SqlCreds.from_url(self.url)
        conn = engine.connect()
        try:
            tbl_size = ms.table_data_size(table, conn)
            if tbl_size == 0:
                rmtempfifo(self._fifo)
                LOG.debug(f"Table {table!r} is empty")
                raise ms.ProgrammingError(f"no result set")
        except Exception as e:
            rmtempfifo(self._fifo)
            raise e
        else:
            self._bcp = BCP.cmd(
                table,
                self.url,
                flat_file=self._fifo,
                format=self.format,
                field_terminator=ft,
                row_terminator=rt,
                shell=True,
                conn=conn,
            )
            self.columns = ms.table_columns(table, conn)
        finally:
            conn.close()

        upload_cmd = self.sink.cmd()
        if self.enable_compress:
            compress_cmd = self.compress.cmd(level=self.compress_level)
            split_filter = cmd2pipe(compress_cmd, upload_cmd)
        else:
            split_filter = upload_cmd
        split_cmd = Split.cmd(filter=split_filter, lines=self.chunk_size)
        cat_cmd = Cat.cmd(self._fifo)
        cmds = [cat_cmd, split_cmd]
        self.add_escape(cmds)
        self._pipe = cmd2pipe(*cmds)

    def pump(self):
        if self.bcp is None or self.pipe is None:
            raise ValueError("BCP or pipe is not built")

        LOG.debug(f"Command BCP: {self.bcp}")
        LOG.debug(f"Command pipe: {self.pipe}")

        if not os.path.exists(self._fifo):
            raise RuntimeError(f"fifo not created: {self._fifo}")

        pipe_proc = Popen(self.pipe, shell=True)
        pp = psutil.Process(pipe_proc.pid)
        LOG.debug(
            f"Sink started, name: {pp.name()}\tpid: {pp.pid}\tppid: {pp.ppid()}\t"
            f"exe: {pp.exe()}\tcmdline: {pp.cmdline()}"
        )

        bcp_proc = Popen(self.bcp, shell=True)
        p = psutil.Process(bcp_proc.pid)
        LOG.debug(
            f"Source started, name: {p.name()}\tpid: {p.pid}\tppid: {p.ppid()}\t"
            f"exe: {p.exe()}\tcmdline: {p.cmdline()}"
        )

        returncode = bcp_proc.wait()
        LOG.debug(f"BCP exited: {returncode}")
        if returncode != 0:
            pp.kill()
            raise RuntimeError(f"BCP download failed")
