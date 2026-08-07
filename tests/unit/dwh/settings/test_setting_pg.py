import pytest
from pydantic import ValidationError

from dwh.settings.setting_pg import PostgresSettings


@pytest.mark.unit
def test_settings_from_env(monkeypatch):
    """Тест загрузки значений из env"""
    monkeypatch.setenv("PG_HOST", "localhost")
    monkeypatch.setenv("PG_PORT", "5432")
    monkeypatch.setenv("PG_USER", "user")
    monkeypatch.setenv("PG_PASS", "pass")
    monkeypatch.setenv("PG_NAME", "db")

    setting = PostgresSettings(_env_file=None)

    assert setting.HOST == "localhost"
    assert setting.PORT == 5432
    assert setting.USER == "user"
    assert setting.PASS == "pass"
    assert setting.NAME == "db"


@pytest.mark.unit
def test_pg_url_property(monkeypatch):
    """Тест свойства pg_url"""
    monkeypatch.setenv("PG_HOST", "localhost")
    monkeypatch.setenv("PG_PORT", "5432")
    monkeypatch.setenv("PG_USER", "user")
    monkeypatch.setenv("PG_PASS", "pass")
    monkeypatch.setenv("PG_NAME", "db")

    setting = PostgresSettings(_env_file=None)
    expected_url = "postgresql+psycopg://user:pass@localhost:5432/db"
    assert setting.pg_url == expected_url


@pytest.mark.unit
def test_missing_required_field(monkeypatch):
    """Если не задано обязательное поле"""
    # PG_HOST пропущен
    monkeypatch.setenv("PG_PORT", "5432")
    monkeypatch.setenv("PG_USER", "user")
    monkeypatch.setenv("PG_PASS", "pass")
    monkeypatch.setenv("PG_NAME", "db")

    with pytest.raises(ValidationError):
        PostgresSettings(_env_file=None)
