import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from streaming.producers.producer_crypto.base import MarketStream
from streaming.producers.producer_crypto.state import exchange_state


# Тестовый подкласс с реализацией абстрактных методов
class TestMarketStream(MarketStream):
    __test__ = False  # чтобы pytest не пытался собирать его как тест

    def process_message(self, raw: dict):
        pass

    def build_sub_messages(self, batch):
        return {"method": "SUBSCRIBE", "params": batch}

    def create_batches_pairs(self, pairs):
        return [pairs[i : i + 2] for i in range(0, len(pairs), 2)]


@pytest.fixture
def stream_instance():
    obj = TestMarketStream(
        source_name="test_exchange",
        market_type="spot",
        pairs=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
        producer=AsyncMock(),
        add_attempt_script=AsyncMock(),
        add_connection_script=AsyncMock(),
        ws_url="ws://test",
        limit_connections=10,
        limit_attempt=5,
        ttl_attempt=60,
        queue_size=1000,
        filter_ping_pong=None,
        ping_interval=20,
    )
    return obj


@pytest.mark.asyncio
async def test_run_creates_components_and_starts_tasks(stream_instance):
    exchange_state.clear()
    stream_instance.create_batches_pairs = MagicMock(
        return_value=[["BTCUSDT"], ["ETHUSDT", "BNBUSDT"]]
    )

    with patch(
        "streaming.producers.producer_crypto.base.KafkaWriter"
    ) as MockKafkaWriter, patch(
        "streaming.producers.producer_crypto.base.RateLimiter"
    ) as MockRateLimiter, patch(
        "streaming.producers.producer_crypto.base.WSConnectionHandler"
    ) as MockWSHandler:
        mock_writer_instance = AsyncMock()
        MockKafkaWriter.return_value = mock_writer_instance

        mock_limiter_instance = AsyncMock()
        mock_limiter_instance.access = AsyncMock(return_value=True)
        MockRateLimiter.return_value = mock_limiter_instance

        mock_handler_instance = AsyncMock()
        mock_handler_instance.run = AsyncMock(return_value=None)
        MockWSHandler.return_value = mock_handler_instance

        await stream_instance.run()

        stream_instance.create_batches_pairs.assert_called_once_with(
            stream_instance.pairs
        )

        key = f"{stream_instance.source_name}-{stream_instance.market_type}"
        assert key in exchange_state
        exchange_info = exchange_state[key]
        assert exchange_info.exchange == stream_instance.source_name
        assert exchange_info.market_type == stream_instance.market_type
        assert exchange_info.expected_ws == 2

        MockKafkaWriter.assert_called_once_with(
            queue=stream_instance.queue,
            producer=stream_instance.producer,
            exchange_info=exchange_info,
        )

        MockRateLimiter.assert_called_once_with(
            main_key=stream_instance.source_name,
            attempt_script=stream_instance.add_attempt_script,
            connection_script=stream_instance.add_connection_script,
            limit_attempt=stream_instance.limit_attempt,
            limit_connections=stream_instance.limit_connections,
            ttl_attempt=stream_instance.ttl_attempt,
        )

        calls = MockWSHandler.call_args_list
        assert len(calls) == 2
        _, kwargs1 = calls[0]
        assert kwargs1["batch"] == ["BTCUSDT"]
        assert kwargs1["limiter"] == mock_limiter_instance
        assert kwargs1["ws_url"] == stream_instance.ws_url
        _, kwargs2 = calls[1]
        assert kwargs2["batch"] == ["ETHUSDT", "BNBUSDT"]
        assert kwargs2["limiter"] == mock_limiter_instance
