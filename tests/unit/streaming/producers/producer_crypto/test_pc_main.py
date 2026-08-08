import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from streaming.producers.producer_crypto.main import main


@pytest.mark.unit
@pytest.mark.asyncio
async def test_main():
    with patch("streaming.producers.producer_crypto.main.READY_EXCHANGES", ["binance"]):
        with patch(
            "streaming.producers.producer_crypto.main.run_server",
            new_callable=AsyncMock,
        ):
            with patch(
                "streaming.producers.producer_crypto.main.kafka_utils.get_bootstrap_servers",
                return_value="localhost:9092",
            ):
                with patch(
                    "streaming.producers.producer_crypto.main.kafka_utils.wait_kafka",
                    new_callable=AsyncMock,
                ):
                    with patch(
                        "streaming.producers.producer_crypto.main.kafka_utils.create_producer",
                        return_value=AsyncMock(),
                    ) as mock_create_producer:
                        with patch(
                            "streaming.producers.producer_crypto.main.load_config_sources",
                            return_value=[
                                {"name_exchange": "binance", "type_markets": ["spot"]}
                            ],
                        ):
                            with patch(
                                "streaming.producers.producer_crypto.main.kafka_utils.exists_topic",
                                new_callable=AsyncMock,
                                return_value=True,
                            ):
                                with patch(
                                    "streaming.producers.producer_crypto.main.kafka_utils.create_topic",
                                    new_callable=AsyncMock,
                                ):
                                    with patch(
                                        "streaming.producers.producer_crypto.main.select_pairs",
                                        return_value=["BTCUSDT"],
                                    ):
                                        with patch(
                                            "streaming.producers.producer_crypto.main.exchanges"
                                        ) as mock_exchanges:
                                            mock_stream = AsyncMock()
                                            mock_exchanges.Binance = MagicMock(
                                                return_value=mock_stream
                                            )
                                            await main()
                                            mock_create_producer.return_value.start.assert_awaited_once()
                                            mock_stream.run.assert_awaited_once()
