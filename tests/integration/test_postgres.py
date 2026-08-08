import pytest

from sqlalchemy import text
from unittest.mock import MagicMock

from dwh.init_dwh.main import ddl_postgres, dml_postgres
from dwh.queries.core.dql import select_pairs


@pytest.mark.integration
def test_ddl_postgres(postgres_container):
    """Проверяет, что ddl_postgres создаёт схему crypto и таблицу pairs."""
    ddl_postgres(postgres_container)
    with postgres_container.connect() as conn:
        result = conn.execute(
            text(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'crypto'"
            )
        )
        assert result.scalar() == "crypto", "Схема crypto не создана"

        result = conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'crypto' AND table_name = 'pairs'"
            )
        )
        assert result.scalar() == "pairs", "Таблица pairs не создана"

        result = conn.execute(
            text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'crypto' AND table_name = 'pairs'
        """)
        )
        columns = {row[0]: row[1] for row in result}
        expected_columns = {
            "ccxt_symbol": "character varying",
            "exchange": "character varying",
            "symbol_exchange_rest": "character varying",
            "symbol_exchange_websocket": "character varying",
            "type_market": "character varying",
            "base": "character varying",
            "quote": "character varying",
        }

        for col, dtype in expected_columns.items():
            assert col in columns, f"Колонка {col} отсутствует"
            assert (
                dtype == columns[col]
            ), f"Тип колонки - {col} ({columns[col]}) не соответствует ожидаемому ({expected_columns[col]})"


@pytest.mark.integration
def test_dml_postgres(postgres_container):
    expected_result = [
        {
            "ccxt_symbol": "BTCETH",
            "exchange": "binance",
            "symbol_exchange_rest": "BTC/ETH",
            "symbol_exchange_websocket": "BTC/ETH",
            "type_market": "spot",
            "base": "BTC",
            "quote": "ETH",
        }
    ]

    mock_rest_get_pairs = MagicMock(side_effect=expected_result)

    dml_postgres(
        sync_pg_engine=postgres_container, rest_get_pairs_func=mock_rest_get_pairs
    )


@pytest.mark.integration
def test_dql_postgres(postgres_container):
    result = select_pairs(
        sync_pg_engine=postgres_container, exchange="binance", type_market="spot"
    )

    assert result == ["BTC/ETH"]
