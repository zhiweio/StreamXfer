import os
import shlex
import sys
import tempfile
import time

from streamxfer.typing import *

IS_WIN32 = sys.platform == "win32"
IS_MACOS = sys.platform == "darwin"


def mkfifo(tmpf):
    if os.path.exists(tmpf):
        return
    os.mkfifo(tmpf, mode=0o777)


def mktempfifo(suffix=""):
    d = tempfile.mkdtemp()
    fifo = os.path.join(d, "fifo") + suffix
    os.mkfifo(fifo)
    return fifo


def wait_until_created(filename, retry=30):
    while retry > 0:
        if os.path.exists(filename):
            return
        time.sleep(0.1)
        retry -= 1


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


def contains_dot(columns: List[Dict]):
    for col in columns:
        if "." in col["column_name"]:
            return True
    return False


def mask_dot_name(name: str) -> str:
    return name.replace(".", "||")


def unmask_dot(json_data: Dict):
    data = dict()
    for col_name, col_value in json_data.items():
        col_name = col_name.replace("||", ".")
        data[col_name] = col_value
    return data
