"""Helpers for logging values copied from matched Kafka messages."""


def safe_log_value(value: object, *, limit: int = 200) -> str:
    """Bound and escape untrusted values before interpolating them in logs.

    Truncation happens only between complete escaped tokens, so a log value
    never ends with a partial ``\\uXXXX`` sequence. Backslashes are escaped to
    keep a literal ``\\u000a`` distinguishable from an encoded line break.
    """
    if limit <= 0:
        return ""
    text = "" if value is None else str(value)
    chunks: list[str] = []
    size = 0
    for char in text:
        if char == "\\":
            token = "\\\\"
        elif char.isprintable() and char not in "\r\n\t":
            token = char
        else:
            token = f"\\u{ord(char):04x}"
        if size + len(token) > limit:
            marker = "…"
            if size + len(marker) <= limit:
                chunks.append(marker)
            break
        chunks.append(token)
        size += len(token)
    return "".join(chunks)
