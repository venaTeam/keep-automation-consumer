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
