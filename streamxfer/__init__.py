import os
from subprocess import Popen

import psutil
from sqlalchemy import create_engine

from streamxfer import mssql as ms
from streamxfer.cmd import (
    Compress,
    BCP,
    Split,
    Cat,
    RedshiftEscape,
    MssqlCsvEscape,
    MssqlJsonEscape,
)
from streamxfer.compress import COMPRESS_LEVEL
from streamxfer.format import Format, sc
from streamxfer.log import LOG
from streamxfer.utils import mktempfifo, cmd2pipe, wait_until_created, contains_dot

__module__ = ["StreamXfer"]
__all__ = ["StreamXfer"]


class StreamXfer:
    def __init__(
        self,
        url,
        format: str,
        enable_compress=True,
        compress_type: str = Compress.lzop.name,
        compress_level=COMPRESS_LEVEL,
        chunk_size=1000000,
    ):
        self.url = url
        self.format = format
        self.compress_type = compress_type
        self.compress_level = compress_level
        self.enable_compress = enable_compress
        self.chunk_size = chunk_size
        self._bcp = None
        self._pipe = None
        self._fifo = None

    @property
    def bcp(self):
        return self._bcp

    @property
    def pipe(self):
        return self._pipe

    def build(
        self,
        table,
        path: str,
        sink,
        redshift_escape=False,
    ):
        compress = Compress(self.compress_type)
        file_ext = "." + self.format.lower()
        if self.enable_compress:
            file_ext = file_ext + compress.ext()
        self._fifo = mktempfifo(suffix=file_ext)
        uri = os.path.join(path, "$FILE" + file_ext)

        if self.format == Format.CSV:
            ft = ms.csv_in_ft
            rt = ms.csv_in_rt
        else:
            ft = sc.TAB
            rt = sc.LN

        engine = create_engine(self.url)
        conn = engine.connect()
        dot_in_cols = False
        try:
            tbl_size = ms.table_data_size(table, conn)
            if tbl_size == 0:
                LOG.info(f"Table {table!r} is empty")
                return
        except Exception:
            pass
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
            columns = ms.table_columns(table, conn)
            dot_in_cols = contains_dot(columns)
        finally:
            conn.close()

        upload_cmd = sink.cmd(uri)
        compress_cmd = compress.cmd(level=self.compress_level)
        if self.enable_compress:
            split_filter = cmd2pipe(compress_cmd, upload_cmd)
        else:
            split_filter = upload_cmd
        split_cmd = Split.cmd(filter=split_filter, lines=self.chunk_size)
        cat_cmd = Cat.cmd(self._fifo)
        cmds = [cat_cmd, split_cmd]
        if self.format == Format.TSV and redshift_escape:
            cmds.insert(1, RedshiftEscape.cmd(shell=True))
        elif self.format == Format.CSV:
            cmds.insert(1, MssqlCsvEscape.cmd(shell=True))
        elif self.format == Format.JSON and dot_in_cols:
            cmds.insert(1, MssqlJsonEscape.cmd(shell=True))

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
