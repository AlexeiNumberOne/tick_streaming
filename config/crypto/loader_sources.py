import json
import os

from pathlib import Path
from pydantic import BaseModel, field_validator


class MarketConfig(BaseModel):
    type: str
    enable: bool

    @field_validator("type")
    @classmethod
    def validate_market_type(cls, v: str) -> str:
        allowed = {"spot", "swap", "future", "option"}
        if v not in allowed:
            raise ValueError(f"Неизвестный тип рынка: '{v}'. Допустимы: {allowed}")
        return v


class ExchangeConfig(BaseModel):
    name: str
    markets: list[MarketConfig]

    @field_validator("name")
    @classmethod
    def validate_exchange_name(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Имя биржи не может быть пустым")
        return v


class SourcesConfig(BaseModel):
    sources: list[ExchangeConfig]


def load_config_sources(
    config_path: Path = None,
    valid_exchanges: list[
        str
    ] = None,  # Producer должен передать готовые биржи(классы), с котоырыми может работать
) -> list[dict]:
    """
    Загружает и валидирует конфиг источников.
    Возвращает список словарей
    [
        {
            name_exchange: 'binance',
            type_markets: [spot,...]
        },
        ...
    ]
    """
    if config_path is None:
        config_path = Path(
            os.environ.get("PATH_CONFIG_SOURCES", "config/crypto/sources.json")
        )

    if not config_path.exists():
        raise FileNotFoundError(f"Файл конфига не найден: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    try:
        config = SourcesConfig(**raw_data)
    except Exception as e:
        raise ValueError(f"Ошибка валидации конфига: {e}") from e

    if valid_exchanges is not None:
        for exchange in config.sources:
            if exchange.name not in valid_exchanges:
                raise ValueError(
                    f"Неизвестная биржа '{exchange.name}'. Доступные: {', '.join(valid_exchanges)}"
                )

    result = []
    for exchange in config.sources:
        enabled_markets = [m.type for m in exchange.markets if m.enable]
        if enabled_markets:
            result.append(
                {"name_exchange": exchange.name, "type_markets": enabled_markets}
            )

    return result
