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
