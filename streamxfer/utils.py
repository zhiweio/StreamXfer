import os
import shlex
import sys
import tempfile
from streamxfer.typing import *

IS_WIN32 = sys.platform == "win32"


def mkfifo(tmpf):
    if os.path.exists(tmpf):
        return
    os.mkfifo(tmpf, mode=0o777)


def mktempfifo(suffix=""):
    d = tempfile.mkdtemp()
    return os.path.join(d, "fifo") + suffix


def quote_this(this: str) -> str:
    """
    OS-safe way to quote a string.
    Returns the string with quotes around it.
    On Windows ~~it's double quotes~~ we skip quoting,
    on Linux it's single quotes.
    """
    if isinstance(this, str):
        if IS_WIN32:
            return this  # TODO maybe change?
        else:
            return shlex.quote(this)
    else:
        return this


def cmd2pipe(*cmds: Union[List[str], str]):
    cmds = [" ".join(cmd) if isinstance(cmd, list) else cmd for cmd in cmds]
    return " | ".join(cmds)
