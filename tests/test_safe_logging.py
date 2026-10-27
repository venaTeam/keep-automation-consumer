from src.core.safe_logging import safe_log_value


def test_safe_log_value_escapes_control_characters():
    assert safe_log_value("evt-1\r\nforged=warning\tvalue") == (
        "evt-1\\u000d\\u000aforged=warning\\u0009value"
    )


def test_safe_log_value_bounds_untrusted_values():
    assert safe_log_value("x" * 250, limit=20) == "x" * 20
