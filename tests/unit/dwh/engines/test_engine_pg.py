from unittest.mock import patch, MagicMock
import pytest
from sqlalchemy.engine import Engine
from dwh.engines.engine_pg import get_sync_pg_engine


@pytest.mark.unit
def test_get_sync_pg_engine(monkeypatch):
    monkeypatch.setenv("PG_HOST", "localhost")
    monkeypatch.setenv("PG_PORT", "5432")
    monkeypatch.setenv("PG_USER", "test_user")
    monkeypatch.setenv("PG_PASS", "secret")
    monkeypatch.setenv("PG_NAME", "test_db")

    with patch("dwh.engines.engine_pg.create_engine") as mock_create_engine:
        fake_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = fake_engine

        result = get_sync_pg_engine()

        expected_url = "postgresql+psycopg://test_user:secret@localhost:5432/test_db"
        mock_create_engine.assert_called_once_with(url=expected_url)
        assert result is fake_engine
