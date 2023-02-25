import os
from subprocess import Popen

from sqlalchemy import create_engine

from streamxfer import mssql as ms
from streamxfer.cmd import (
    Compress,
    BCP,
    Split,
    Cat,
    RedshiftEscape,
    MssqlCsvEscape,
)
from streamxfer.compress import COMPRESS_LEVEL
from streamxfer.format import Format, sc
from streamxfer.log import LOG
from streamxfer.utils import cmd2pipe
from streamxfer.utils import mktempfifo, cmd2pipe

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
        file_ext = "." + self.format.lower()
        if self.enable_compress:
            file_ext = file_ext + "." + self.compress_type.lower()
        flat_file = mktempfifo(suffix=file_ext)
        uri = os.path.join(path, "$FILE" + file_ext)
        compress = Compress(self.compress_type)

        if self.format == Format.CSV:
            ft = ms.csv_in_ft
            rt = ms.csv_in_rt
        else:
            ft = sc.TAB
            rt = sc.LN

        engine = create_engine(self.url)
        conn = engine.connect()
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
                flat_file=flat_file,
                format=self.format,
                field_terminator=ft,
                row_terminator=rt,
                shell=True,
                conn=conn,
            )
        finally:
            conn.close()

        upload_cmd = sink.cmd(uri)
        compress_cmd = compress.cmd(level=self.compress_level)
        if self.enable_compress:
            split_filter = cmd2pipe(compress_cmd, upload_cmd)
        else:
            split_filter = upload_cmd
        split_cmd = Split.cmd(filter=split_filter, lines=self.chunk_size)
        cat_cmd = Cat.cmd(flat_file)
        cmds = [cat_cmd, split_cmd]
        if format == Format.TSV and redshift_escape:
            cmds.insert(1, RedshiftEscape.cmd(shell=True))
        elif format == Format.CSV:
            cmds.insert(1, MssqlCsvEscape.cmd(shell=True))

        self._pipe = cmd2pipe(*cmds)
        LOG.debug(f"stream pipe: {self.pipe}")

    def pump(self):
        if self.bcp is None or self.pipe is None:
            raise ValueError("BCP or pipe is not built")

        with Popen(self.bcp) as bcp_proc:
            LOG.info(f"BCP process started, pid: {bcp_proc.pid}")
            with Popen(self.pipe) as pipe_proc:
                LOG.info(f"pipe built, pid: {pipe_proc.pid}")

            bcp_proc.wait()
            if bcp_proc.returncode != 0:
                raise RuntimeError(f"BCP download failed")
