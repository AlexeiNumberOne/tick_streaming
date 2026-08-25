# import os

# KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP")
# KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC", "binance")

INTERVAL_MAP = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}

DEFAULT_INTERVALS = list(INTERVAL_MAP.keys())
