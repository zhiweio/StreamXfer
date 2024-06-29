import os
from pathlib import Path

from streamxfer.typing import *
from streamxfer.cmd import Cat


class BaseSink:
    bin = None

    def __init__(self, uri: str) -> None:
        self._uri = os.path.join(uri, "$FILE")

    @property
    def uri(self) -> str:
        return self._uri

    def set_file_extension(self, ext: str) -> None:
        self._uri += ext

    def cmd(self) -> Union[List[str], str]:
        raise NotImplementedError


class LocalSink(BaseSink):
    bin = Cat.bin

    def cmd(self) -> Union[List[str], str]:
        _cmd = [self.bin, ">", self.uri]
        return " ".join(_cmd)


class S3Sink(BaseSink):
    bin = "aws s3 cp"

    def cmd(self) -> Union[List[str], str]:
        _cmd = [self.bin, "-", self.uri]
        return " ".join(_cmd)


def uri2sink(uri):
    if uri.startswith("s3://"):
        return S3Sink(uri)
    else:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        return LocalSink(uri)
