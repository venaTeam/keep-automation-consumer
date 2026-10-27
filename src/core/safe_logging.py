"""Helpers for logging values copied from matched Kafka messages."""


def safe_log_value(value: object, *, limit: int = 200) -> str:
    """Bound and escape untrusted values before interpolating them in logs."""
    text = "" if value is None else str(value)
    escaped = "".join(
        char if char.isprintable() and char not in "\r\n\t" else f"\\u{ord(char):04x}"
        for char in text
    )
    return escaped[:limit]
