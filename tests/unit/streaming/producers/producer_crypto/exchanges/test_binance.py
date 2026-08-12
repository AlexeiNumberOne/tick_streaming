import pytest
from unittest.mock import MagicMock
from streaming.producers.producer_crypto.exchanges.binance import Binance

MOCK_PRODUCER = MagicMock()
MOCK_ADD_ATTEMPT_SCRIPT = MagicMock()
MOCK_ADD_CONNECTION_SCRIPT = MagicMock()

correct_kwargs = {
    "market_type": "spot",
    "source_name": "binance",
    "pairs": ["BTCUSDT", "ETHUSDT"],
    "producer": MOCK_PRODUCER,
    "add_attempt_script": MOCK_ADD_ATTEMPT_SCRIPT,
    "add_connection_script": MOCK_ADD_CONNECTION_SCRIPT,
}


@pytest.mark.unit
def test_binance_constructor_type_market_binance():
    reference_markets = {
        "spot": "wss://stream.binance.com:9443/ws",
        "swap": "wss://fstream.binance.com/ws",
        "future": "wss://dstream.binance.com/ws",
    }

    for market, ref in reference_markets.items():
        stream = Binance(
            market_type=market,
            source_name="binance",
            pairs=["BTCUSDT", "ETHUSDT"],
            producer=MOCK_PRODUCER,
            add_attempt_script=MOCK_ADD_ATTEMPT_SCRIPT,
            add_connection_script=MOCK_ADD_CONNECTION_SCRIPT,
        )
        assert stream.market_type == market
        assert stream.ws_url == ref


@pytest.mark.unit
def test_binance_constructor_wrong_type_market_binance():
    market = "None"

    with pytest.raises(ValueError, match="Неизвестный market_type"):
        stream = Binance(
            market_type=market,
            source_name="binance",
            pairs=["BTCUSDT", "ETHUSDT"],
            producer=MOCK_PRODUCER,
            add_attempt_script=MOCK_ADD_ATTEMPT_SCRIPT,
            add_connection_script=MOCK_ADD_CONNECTION_SCRIPT,
        )
        stream


@pytest.mark.unit
def test_create_batches_pairs_and_build_sub_messages():
    stream = Binance(**correct_kwargs)
    stream.LIMIT_SUB = 1
    batches = stream.create_batches_pairs(stream.pairs)

    assert batches == [["BTCUSDT"], ["ETHUSDT"]]

    result = []
    for batch in batches:
        result.append(stream.build_sub_messages(batch))

    excepted = [
        {"method": "SUBSCRIBE", "params": ["btcusdt@trade"], "id": 1},
        {"method": "SUBSCRIBE", "params": ["ethusdt@trade"], "id": 1},
    ]

    assert result == excepted


@pytest.mark.unit
def test_process_message_buy():
    stream = Binance(**correct_kwargs)

    raw = {
        "s": "BTCUSDT",
        "p": "12345.67",
        "q": "1.234",
        "T": 1623456789000,
        "t": 123456,
        "m": False,
    }
    result = list(stream.process_message(raw))
    assert len(result) == 1
    item = result[0]
    assert item["symbol"] == "BTCUSDT"
    assert item["side"] == "buy"


@pytest.mark.unit
def test_process_message_sell():
    stream = Binance(**correct_kwargs)
    raw = {
        "s": "BTCUSDT",
        "p": "12345.67",
        "q": "1.234",
        "T": 1623456789000,
        "t": 123456,
        "m": True,
    }
    result = list(stream.process_message(raw))
    assert len(result) == 1
    item = result[0]
    assert item["symbol"] == "BTCUSDT"
    assert item["side"] == "sell"


@pytest.mark.unit
def test_process_message_key_error():
    stream = Binance(**correct_kwargs)
    raw = {}
    result = list(stream.process_message(raw))
    assert result == []


@pytest.mark.unit
def test_process_message_value_error():
    stream = Binance(**correct_kwargs)
    raw = {
        "s": "BTCUSDT",
        "p": "1",
        "q": "1.234",
        "T": "1623456789000",
        "t": "a-123456",
        "m": "a",  # not bool
    }
    result = list(stream.process_message(raw))
    assert result == []
