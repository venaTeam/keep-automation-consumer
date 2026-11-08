"""Configuration bootstrap tests."""

import importlib


def test_dotenv_is_loaded_before_constants_are_evaluated(tmp_path, monkeypatch):
    from src.config import config as config_module
    from src.config import consts

    (tmp_path / ".env").write_text("REDIS_URL=redis://dotenv:6379\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("REDIS_URL", raising=False)

    importlib.reload(config_module)
    importlib.reload(consts)

    assert consts.REDIS_URL == "redis://dotenv:6379"

    monkeypatch.setenv("REDIS_URL", "")
    importlib.reload(config_module)
    importlib.reload(consts)



def test_submit_timeouts_stay_under_the_provisional_cooldown_ttl(monkeypatch):
    """An `accepted` arriving after the 30s provisional claim expired could no
    longer arm the cooldown, so the timeouts are clamped below it."""
    from src.bl.gates.cooldown_key import COOLDOWN_PROVISIONAL_TTL_SECONDS
    from src.config import config as config_module
    from src.config import consts

    monkeypatch.setenv("AUTOMATION_API_TIMEOUT_SECONDS", "2000")
    monkeypatch.setenv("AUTOMATION_API_CONNECT_TIMEOUT_SECONDS", "100")
    monkeypatch.setenv("SUBMIT_RETRY_BACKOFF_INITIAL_SECONDS", "0")
    try:
        importlib.reload(config_module)
        importlib.reload(consts)

        assert consts.AUTOMATION_API_TIMEOUT_SECONDS == 12.0
        assert consts.AUTOMATION_API_CONNECT_TIMEOUT_SECONDS == 5.0
        # httpx applies the timeout per phase: connect + write + read.
        assert (
            consts.AUTOMATION_API_CONNECT_TIMEOUT_SECONDS
            + 2 * consts.AUTOMATION_API_TIMEOUT_SECONDS
            < COOLDOWN_PROVISIONAL_TTL_SECONDS
        )
        assert consts.IN_FLIGHT_DRAIN_TIMEOUT_SECONDS == 34.0
        # No zero-delay retry storm against a struggling API.
        assert consts.SUBMIT_RETRY_BACKOFF_INITIAL_SECONDS == 0.1
    finally:
        monkeypatch.delenv("AUTOMATION_API_TIMEOUT_SECONDS")
        monkeypatch.delenv("AUTOMATION_API_CONNECT_TIMEOUT_SECONDS")
        monkeypatch.delenv("SUBMIT_RETRY_BACKOFF_INITIAL_SECONDS")
        importlib.reload(config_module)
        importlib.reload(consts)
