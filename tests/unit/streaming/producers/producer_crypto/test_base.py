import pytest

from unittest.mock import AsyncMock, MagicMock, patch

from streaming.producers.producer_crypto.base import MarketStream
# from streaming.producers.producer_crypto.exchanges.binance import Binance

MOCK_PRODUCER = MagicMock()
MOCK_ADD_ATTEMPT_SCRIPT = MagicMock()
MOCK_ADD_CONNECTION_SCRIPT = MagicMock()

correct_kwargs = {
    "market_type": "spot",
    "source_name": "binance",
    "pairs": ["BTCUSDT", "ETHUSDT"],
    "producer": MOCK_PRODUCER,
    "topic": "binance",
    "add_attempt_script": MOCK_ADD_ATTEMPT_SCRIPT,
    "add_connection_script": MOCK_ADD_CONNECTION_SCRIPT,
    "ready_websockets": {},
}


class mock_subclass(MarketStream):
    def __init__(
        self,
        source_name,
        market_type,
        pairs,
        topic,
        producer,
        add_attempt_script,
        add_connection_script,
        ready_websockets,
    ):
        super().__init__(
            source_name=source_name,
            market_type=market_type,
            topic=topic,
            producer=producer,
            add_attempt_script=add_attempt_script,
            add_connection_script=add_connection_script,
            ws_url="",
            limit_connections=10,
            limit_attempt=10,
            ttl_attempt=10,
            ready_websockets=ready_websockets,
        )

    def process_message():
        pass

    def build_sub_messages():
        pass


@pytest.mark.unit
@pytest.mark.asyncio
async def test_reader_creates_tasks_and_excpected():
    stream = mock_subclass(**correct_kwargs)

    for mock_messages in [[{}, {}], [{}]]:
        stream.build_sub_messages = MagicMock(return_value=mock_messages)

        mock_task = AsyncMock()
        stream._create_connection = AsyncMock(return_value=mock_task)

        await stream._reader()
        stream.build_sub_messages.assert_called_once()
        assert stream._create_connection.call_count == len(mock_messages)
        assert stream.stats_init_websockets["expected"] == len(mock_messages)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_request_to_redis_attempt_True_connection_True():
    stream = mock_subclass(**correct_kwargs)
    stream.add_attempt_script = AsyncMock(return_value=True)
    stream.add_connection_script = AsyncMock(return_value=True)

    result = await stream._request_to_redis()
    assert result is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_request_to_redis_attempt_5_attempt_True_connection_True():
    stream = mock_subclass(**correct_kwargs)
    stream.add_attempt_script = AsyncMock(side_effect=[5, True])
    stream.add_connection_script = AsyncMock(return_value=True)
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        result = await stream._request_to_redis()
        mock_sleep.assert_awaited_once_with(5 + 3)
        assert result is True
        assert stream.add_attempt_script.call_count == 2
        assert stream.add_connection_script.call_count == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_request_to_redis_connection_False():
    stream = mock_subclass(**correct_kwargs)
    stream.add_attempt_script = AsyncMock(return_value=True)
    stream.add_connection_script = AsyncMock(return_value=False)
    result = await stream._request_to_redis()
    assert result is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_request_to_redis_wrong_attempt():
    stream = mock_subclass(**correct_kwargs)
    with pytest.raises(ValueError, match="Неожиданный ответ от lua скрипта!"):
        stream.add_attempt_script = AsyncMock(return_value=None)
        await stream._request_to_redis()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_request_to_redis_wrong_connection():
    stream = mock_subclass(**correct_kwargs)
    stream.add_attempt_script = AsyncMock(return_value=True)
    with pytest.raises(ValueError, match="Неожиданный ответ от lua скрипта!"):
        stream.add_connection_script = AsyncMock(return_value=None)
        await stream._request_to_redis()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_request_to_redis_timeout():
    stream = mock_subclass(**correct_kwargs)
    stream.add_attempt_script = AsyncMock(return_value=5)
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        mock_loop = MagicMock()
        mock_loop.time.return_value = 10
        with patch("asyncio.get_event_loop", return_value=mock_loop):
            with pytest.raises(
                TimeoutError, match="Таймаут ожидания слота для соединения"
            ):
                await stream._request_to_redis(timeout=0)
                mock_sleep.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_check_access_exception_TimeoutError():
    stream = mock_subclass(**correct_kwargs)
    stream._request_to_redis = AsyncMock(side_effect=TimeoutError("test timeout"))
    result = await stream._check_access()
    assert result is False

    expected = {"active": 0, "rejected": 0, "timeout": 1, "expected": -1}
    assert stream.stats_init_websockets == expected


@pytest.mark.unit
@pytest.mark.asyncio
async def test_check_access_raise_ValueError():
    stream = mock_subclass(**correct_kwargs)
    stream._request_to_redis = AsyncMock(return_value=None)
    with pytest.raises(
        ValueError, match="Пришёл неверный тип данных от _request_to_redis:"
    ):
        await stream._check_access()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_check_access_if_result_False():
    stream = mock_subclass(**correct_kwargs)
    stream._request_to_redis = AsyncMock(return_value=False)

    result = await stream._check_access()
    assert result is False

    expected = {"active": 0, "rejected": 1, "timeout": 0, "expected": -1}

    assert stream.stats_init_websockets == expected


@pytest.mark.unit
@pytest.mark.asyncio
async def test_check_access_if_result_True():
    stream = mock_subclass(**correct_kwargs)
    stream._request_to_redis = AsyncMock(return_value=True)

    result = await stream._check_access()
    assert result is True

    expected = {"active": 1, "rejected": 0, "timeout": 0, "expected": -1}

    assert stream.stats_init_websockets == expected


@pytest.mark.unit
@pytest.mark.asyncio
async def test_check_access_if_result_true_and_ready_websockets_True():
    stream = mock_subclass(**correct_kwargs)
    stream._request_to_redis = AsyncMock(return_value=True)

    stream.ready_websockets["binance-spot"] = False
    stream.stats_init_websockets = {
        "active": 0,
        "rejected": 0,
        "timeout": 0,
        "expected": 1,
    }

    result = await stream._check_access()
    assert result is True

    expected = {"active": 1, "rejected": 0, "timeout": 0, "expected": 0}

    assert stream.stats_init_websockets == expected
    assert stream.ready_websockets["binance-spot"] is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_connection_result_ValueError():
    stream = mock_subclass(**correct_kwargs)
    stream._request_to_redis = AsyncMock(return_value=None)
    with pytest.raises(
        ValueError, match="Пришёл неверный тип данных от _request_to_redis:"
    ):
        await stream._create_connection()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_connection_result_False():
    stream = mock_subclass(**correct_kwargs)
    stream._check_access = AsyncMock(return_value=False)

    result = await stream._create_connection()

    assert result is None
