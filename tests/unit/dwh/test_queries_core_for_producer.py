import pytest

from unittest.mock import MagicMock
from dwh.queries.core_for_producer import select_pairs

from sqlalchemy import MetaData

# metadata_obj_pg = MetaData()


@pytest.mark.unit
def test_select_pairs():
    metadata_obj_pg = MetaData()

    mock_engine = (
        MagicMock()
    )  # Имитация движка SQLAlchemy (объект, который возвращает create_engine())
    mock_conn = (
        MagicMock()
    )  # Имитация соединения с БД (объект, который возвращает engine.connect()).
    mock_result = MagicMock()  # Имитация результата выполнения запроса (объект, который возвращает conn.execute())

    mock_result.scalars().all.return_value = [
        "BTCUSDT"
    ]  # Имитация .scalars().all() - возвращает список ['BTCUSDT']

    # Имитация работы контекстного менеджера
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_engine.connect.return_value.__exit__ = MagicMock()

    mock_conn.execute.return_value = (
        mock_result  # Имитация .execute() - возвращает mock_result
    )

    result = select_pairs(
        sync_pg_engine=mock_engine,
        metadata_obj_pg=metadata_obj_pg,
        exchange="binance",
        type_market="spot",
    )
    assert result == ["BTCUSDT"]

    mock_conn.execute.assert_called_once()


@pytest.mark.unit
def test_select_pairs_returns_empty_list_when_no_data():
    metadata_obj_pg = MetaData()

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.scalars().all.return_value = []

    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_engine.connect.return_value.__exit__ = MagicMock()
    mock_conn.execute.return_value = mock_result

    result = select_pairs(
        sync_pg_engine=mock_engine,
        metadata_obj_pg=metadata_obj_pg,
        exchange="None",
        type_market="None",
    )
    assert result == []
    mock_conn.execute.assert_called_once()
