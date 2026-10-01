from pathlib import Path

import pytest

import airline_warehouse.config as config_module
from airline_warehouse.config import Settings, validate_identifier


def test_identifiers_are_normalized() -> None:
    settings = Settings(account="a", user="u", password="p", database="demo_db")
    assert settings.database == "DEMO_DB"
    assert settings.raw_schema == "RAW"
    assert settings.raw_data_dir.is_absolute()


@pytest.mark.parametrize("value", ["bad-name", "a.b", "drop table x", "1DATABASE"])
def test_unsafe_identifier_is_rejected(value: str) -> None:
    with pytest.raises(ValueError):
        validate_identifier(value, "database")


def test_missing_credentials_are_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_module, "load_dotenv", lambda: None)
    for name in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(Path(__file__).parent)
    with pytest.raises(ValueError, match="SNOWFLAKE_ACCOUNT"):
        Settings.from_env()
