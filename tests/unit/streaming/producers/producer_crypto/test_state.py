import pytest
from streaming.producers.producer_crypto.state import (
    READY_EXCHANGES,
    QUEUE_SIZE,
    STATS_INTERVAL,
)
from streaming.producers.producer_crypto import exchanges


@pytest.mark.unit
def test_ready_exchanges_are_supported():
    for exchange in READY_EXCHANGES:
        class_name = exchange.capitalize()
        assert hasattr(
            exchanges, class_name
        ), f"Для биржи '{exchange}' не найден класс '{class_name}' в модуле exchanges"


@pytest.mark.unit
def test_queue_size():
    assert isinstance(QUEUE_SIZE, int), "QUEUE_SIZE должен быть int"
    assert (
        10000 <= QUEUE_SIZE <= 100000
    ), "QUEUE_SIZE должен быть в диапазоне от 10.000 до 100.000"


@pytest.mark.unit
def test_stats_size():
    assert isinstance(STATS_INTERVAL, int), "STATS_INTERVAL должен быть int"
    assert (
        60 <= STATS_INTERVAL <= 10800
    ), "STATS_INTERVAL должен быть в диапазоне от 60 (сек) до 10.800 (3 часа)"
