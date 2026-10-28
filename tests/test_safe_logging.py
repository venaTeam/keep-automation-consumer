from src.core.safe_logging import safe_log_value


def test_safe_log_value_escapes_control_characters():
    assert safe_log_value("evt-1\r\nforged=warning\tvalue") == (
        "evt-1\\u000d\\u000aforged=warning\\u0009value"
    )


def test_safe_log_value_escapes_backslashes_without_collisions():
    assert safe_log_value("evt\n1") != safe_log_value("evt\\u000a1")


def test_safe_log_value_does_not_split_escape_tokens():
    value = safe_log_value("x" * 197 + "\n")
    assert value.endswith("…")
    assert "\\u0" not in value


def test_safe_log_value_maps_none_to_empty_string():
    assert safe_log_value(None) == ""


def test_safe_log_value_bounds_untrusted_values():
    assert safe_log_value("x" * 250, limit=20) == "x" * 20
