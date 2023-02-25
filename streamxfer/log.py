import os
import sys
from datetime import datetime
from functools import partial
from pathlib import Path

from loguru import logger

LEVEL = os.getenv("LOGURU_LEVEL", "INFO")
DIAGNOSE = False if LEVEL != "DEBUG" else True
logdir = Path(os.path.expanduser("~")).joinpath(".cache/.stx/logging")
logdir.mkdir(parents=True, exist_ok=True)
_log_add = partial(
    logger.add, level=LEVEL, backtrace=True, diagnose=DIAGNOSE, enqueue=True
)

now = datetime.now().strftime("%Y%m%d")
logger.remove()
_log_add(sys.stdout)
_log_add(
    logdir.joinpath(f"stx-{now}.log"),
    filter=lambda record: record["extra"]["task"] == "stx",
)
LOG = logger.bind(task="stx")
