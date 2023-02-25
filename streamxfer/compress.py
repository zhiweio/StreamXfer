from streamxfer.format import MB

COMPRESS_LEVEL = 6
COMPRESS_THRESHOLD = 50 * MB

supported = ["LZOP", "GZIP"]


def enabled(size: int) -> bool:
    return not (size < COMPRESS_THRESHOLD)
