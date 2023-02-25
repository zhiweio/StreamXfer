KB = 1024
MB = 1024 * KB
GB = 1024 * MB


class Format:
    CSV = "CSV"
    TSV = "TSV"
    JSON = "JSON"


supported = [Format.CSV, Format.TSV, Format.JSON]


class SpecialChar:
    LN = "\n"  # New line
    CR = "\r"  # Carriage return
    TAB = "\t"  # Horizontal tab
    FF = "\f"  # Form feed
    BS = "\b"  # Backspace
    SOH = "\001"  # Start of heading
    STX = "\002"  # Start of text
    ETX = "\003"  # End of text
    NUL = "\0"  # Null
    BSL = "\\"  # Backslash


class SpecialAscii:
    LN = b"\x0A"  # New line
    CR = b"\x0D"  # Carriage return
    TAB = b"\x09"  # Horizontal tab
    FF = b"\x0C"  # Form feed
    BS = b"\x08"  # Backspace
    SOH = b"\x01"  # Start of heading
    STX = b"\x02"  # Start of text
    ETX = b"\x03"  # End of text
    NUL = b"\x00"  # Null
    BSL = b"\x5C"  # Backslash


sc = SpecialChar
sa = SpecialAscii


def del_nul(s: str) -> str:
    return s.replace("\0", "")


def redshift_escape(s: str) -> str:
    """Escape special characters in TEXT for Redshift COPY command."""
    return (
        s.replace("\\t", "\\\\t")
        .replace("\\n", "\\\\n")
        .replace("\\r", "\\\\r")
        .replace("\\f", "\\\\f")
        .replace("\\b", "\\\\b")
    )


def read_stream(stream, newline=sa.LN):
    buf = bytearray()
    while True:
        chunk = stream.read(4096)
        if not chunk:
            break
        buf.extend(chunk)
        while newline in buf:
            pos = buf.index(newline)
            yield buf[:pos].decode("utf-8")
            buf = buf[pos + len(newline) :]
    if buf:
        yield buf.decode("utf-8")


if __name__ == "__main__":
    import io
    import timeit

    def test_read_stream():
        stream = io.BytesIO(b"abc\x003def\x003ghi\x003jkl")
        for line in read_stream(stream):
            pass

    n = 1000000000
    t = timeit.timeit(test_read_stream, number=n)
    print(f"执行 {n} 次的平均时间为: {t / n:.6f} 秒")
