import pytest

from dataclasses import dataclass

from streaming.producers.producer_crypto.state import ExchangeInfo


@dataclass
class mock_ws_info:
    health: bool


@dataclass
class mock_ws_manager:
    info: mock_ws_info


@pytest.mark.unit
def test_init_ExchangeInfo():
    mock_ws_1 = mock_ws_manager(mock_ws_info(False))
    result = ExchangeInfo("binance", "spot", 2)
    result.add_manager(mock_ws_1)
    assert len(result.ws) == 1
    assert result.is_ready is False
    mock_ws_1.info.health = True
    assert result.is_ready is False
    mock_ws_2 = mock_ws_manager(mock_ws_info(True))
    result.add_manager(mock_ws_2)
    assert result.is_ready is True
