import pytest

from unittest.mock import MagicMock, patch

from dwh.init_dwh.main import main, ddl_postgres, rest_get_pairs, dml_postgres


@pytest.mark.unit
def test_main():
    mock_pg_engine = MagicMock()
    # mock_ch_engine = MagicMock()
    mock_pg_factory = MagicMock(return_value=mock_pg_engine)
    # mock_ch_factory = MagicMock(return_value=mock_ch_engine)
    mock_ddl_pg = MagicMock()
    # mock_ddl_ch = MagicMock()
    mock_dml_pg = MagicMock()

    main(
        pg_engine_factory=mock_pg_factory,
        # ch_engine_factory=mock_ch_factory,
        ddl_pg=mock_ddl_pg,
        # ddl_ch=mock_ddl_ch,
        dml_pg=mock_dml_pg,
    )

    mock_pg_factory.assert_called_once()
    # mock_ch_factory.assert_called_once()
    mock_ddl_pg.assert_called_once_with(mock_pg_engine)
    # mock_ddl_ch.assert_called_once_with(mock_ch_engine)
    mock_dml_pg.assert_called_once_with(mock_pg_engine)


@pytest.mark.unit
def test_ddl_postgres():
    mock_engine = MagicMock()
    mock_metadata = MagicMock()
    mock_metadata_factory = MagicMock(return_value=mock_metadata)
    mock_make_pairs = MagicMock()
    mock_create_schemas = MagicMock()
    mock_create_tables = MagicMock()

    ddl_postgres(
        sync_pg_engine=mock_engine,
        metadata_factory=mock_metadata_factory,
        make_pairs_func=mock_make_pairs,
        create_schemas=mock_create_schemas,
        create_tables=mock_create_tables,
    )

    mock_create_schemas.assert_called_once_with(sync_pg_engine=mock_engine)
    mock_metadata_factory.assert_called_once()
    mock_make_pairs.assert_called_once_with(metadata_obj_pg=mock_metadata)
    mock_create_tables.assert_called_once_with(
        sync_pg_engine=mock_engine, metadata_obj_pg=mock_metadata
    )


@pytest.mark.unit
def test_rest_get_pairs_success():
    mock_exchange_instance = MagicMock()
    mock_exchange_instance.load_markets.return_value = {
        "BTC/USDT": {
            "id": "btcusdt",
            "symbol": "BTC/USDT",
            "type": "spot",
            "base": "BTC",
            "quote": "USDT",
        },
        "ETH/USDT": {
            "id": "ethusdt",
            "symbol": "ETH/USDT",
            "type": "spot",
            "base": "ETH",
            "quote": "USDT",
        },
    }
    mock_exchange_class = MagicMock(return_value=mock_exchange_instance)

    with patch("dwh.init_dwh.main.ccxt") as mock_ccxt:
        mock_ccxt.binance = mock_exchange_class
        result = rest_get_pairs("binance", ["spot"])
        assert len(result) == 2
        assert result[0]["exchange"] == "binance"
        mock_exchange_instance.load_markets.assert_called_once()


@pytest.mark.unit
def test_dml_postgres():
    mock_engine = MagicMock()
    mock_config_loader = MagicMock(
        return_value=[
            {"name_exchange": "binance", "type_markets": ["spot"]},
            {"name_exchange": "kraken", "type_markets": ["spot"]},
        ]
    )
    mock_metadata = MagicMock()
    mock_metadata_factory = MagicMock(return_value=mock_metadata)
    mock_pairs_table = MagicMock()
    mock_make_pairs = MagicMock(return_value=mock_pairs_table)
    mock_rest_get_pairs = MagicMock(
        side_effect=[
            [{"symbol": "BTC/USDT"}],
            [],  # по kraken вернул пустые данные
        ]
    )
    mock_insert_pairs = MagicMock()

    dml_postgres(
        sync_pg_engine=mock_engine,
        config_loader=mock_config_loader,
        metadata_factory=mock_metadata_factory,
        make_pairs_func=mock_make_pairs,
        rest_get_pairs_func=mock_rest_get_pairs,
        insert_pairs_func=mock_insert_pairs,
    )

    mock_config_loader.assert_called_once()
    mock_metadata_factory.assert_called_once()
    mock_make_pairs.assert_called_once_with(mock_metadata)
    assert mock_rest_get_pairs.call_count == 2
    mock_rest_get_pairs.assert_any_call("binance", ["spot"])
    mock_rest_get_pairs.assert_any_call("kraken", ["spot"])
    mock_insert_pairs.assert_called_once_with(
        table=mock_pairs_table,
        data=[{"symbol": "BTC/USDT"}],
        sync_pg_engine=mock_engine,
    )
