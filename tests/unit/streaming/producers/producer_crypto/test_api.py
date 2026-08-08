import pytest
from streaming.producers.producer_crypto.api import is_ready
from unittest.mock import patch


@pytest.mark.unit
def test_is_ready_true_when_all_producers_ready():
    with patch(
        "streaming.producers.producer_crypto.api.READY_WEBSOCKETS",
        {"binance": True, "bybit": True},
    ):
        assert is_ready() is True


@pytest.mark.unit
def test_is_ready_false_when_one_producer_not_ready():
    with patch(
        "streaming.producers.producer_crypto.api.READY_WEBSOCKETS",
        {"binance": True, "bybit": False},
    ):
        assert is_ready() is False


@pytest.mark.unit
def test_is_ready_when_ready_websockets_not_dict():
    with patch("streaming.producers.producer_crypto.api.READY_WEBSOCKETS", 0):
        with pytest.raises(
            ValueError, match="Неверный тип данных для READY_WEBSOCKETS"
        ):
            is_ready()
