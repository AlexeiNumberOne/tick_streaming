import pytest

from pydantic import ValidationError

from config.crypto.loader_sources import MarketConfig, ExchangeConfig, SourcesConfig


@pytest.mark.unit
def test_market_config_valid():
    m = MarketConfig(type="spot", enable=True)
    assert m.type == "spot"
    assert m.enable is True


@pytest.mark.unit
def test_market_config_invalid_type():
    with pytest.raises(ValidationError, match="Неизвестный тип рынка"):
        MarketConfig(type="unknown", enable=True)


@pytest.mark.unit
def test_exchange_config_empty_name():
    with pytest.raises(ValidationError, match="Имя биржи не может быть пустым"):
        ExchangeConfig(name="", markets=[])


@pytest.mark.unit
def test_sources_config_loads_correctly():
    raw = {
        "sources": [
            {
                "name": "binance",
                "markets": [
                    {"type": "spot", "enable": True},
                    {"type": "swap", "enable": False},
                ],
            }
        ]
    }
    config = SourcesConfig(**raw)
    assert len(config.sources) == 1
    assert config.sources[0].name == "binance"
    assert config.sources[0].markets[0].type == "spot"
