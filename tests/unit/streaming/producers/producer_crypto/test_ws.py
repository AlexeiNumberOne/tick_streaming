import pytest
import websockets
import time
import json

from unittest.mock import AsyncMock, patch
from streaming.producers.producer_crypto.ws import WSInfo, WSConnectionManager


@pytest.mark.unit
def test_default_init_wsinfo():
    result = WSInfo()
    assert result.ws is None
    assert result.health is False
    assert result.created_at is None
    assert result.started_at is None
    assert result.pairs is None
    assert result.sub_msg is None
    assert result.last_tick_time is None
    assert result.reconnects == 0


@pytest.mark.unit
def test_custom_init_wsinfo():
    ws_mock = websockets.ClientConnection
    result = WSInfo(
        ws=ws_mock,
        health=True,
        created_at=int(time.time()),
        started_at=int(time.time()),
        pairs=["BTCUSDT"],
        sub_msg={"method": "SUBSCRIBE", "params": ["btcusdt@trade"], "id": 1},
        last_tick_time=int(time.time()),
        reconnects=0,
    )
    assert result.ws is websockets.ClientConnection
    assert result.health is True
    assert isinstance(result.created_at, int)
    assert isinstance(result.started_at, int)
    assert result.pairs == ["BTCUSDT"]
    assert result.sub_msg == {
        "method": "SUBSCRIBE",
        "params": ["btcusdt@trade"],
        "id": 1,
    }
    assert isinstance(result.last_tick_time, int)
    assert result.reconnects == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_wsconnectionmanager():
    mock_ws = AsyncMock()

    with patch(
        "streaming.producers.producer_crypto.ws.websockets.connect",
        new_callable=AsyncMock,
    ) as mock_connect:
        mock_connect.return_value = mock_ws
        manager = WSConnectionManager("ws://example.com")
        await manager.connect()

        mock_connect.assert_awaited_once_with("ws://example.com", ping_interval=None)
        assert manager.info.ws is mock_ws

    with pytest.raises(ValueError, match="Нет сообщения для подписки"):
        await manager.subscribe()

    mock_ws.recv.return_value = b'{"status": "subscribed"}'
    manager.info.sub_msg = {"method": "SUBSCRIBE", "params": ["btcusdt@trade"], "id": 1}
    await manager.subscribe()
    expected_payload = json.dumps(manager.info.sub_msg)
    mock_ws.send.assert_awaited_once_with(expected_payload)
    mock_ws.recv.assert_awaited_once()

    assert manager.info.started_at is not None
    assert isinstance(manager.info.started_at, int)
    assert manager.info.health is True

    with patch.object(manager, "connect", new_callable=AsyncMock) as mock_connect:
        await manager.reconnect()

        mock_connect.assert_awaited_once()
        assert manager.info.reconnects == 1
