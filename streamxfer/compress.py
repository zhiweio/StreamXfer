from streamxfer.format import MB

COMPRESS_LEVEL = 6
COMPRESS_THRESHOLD = 50 * MB


class CompressType:
    LZOP = "LZOP"
    GZIP = "GZIP"


supported = [CompressType.LZOP, CompressType.GZIP]
