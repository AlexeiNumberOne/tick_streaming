import pytest

from unittest.mock import AsyncMock, MagicMock, patch
from aiokafka.errors import TopicAlreadyExistsError

from streaming.plugins.kafka_utils import (
    get_bootstrap_servers,
    wait_kafka,
    exists_topic,
    create_topic,
    create_producer,
)


@pytest.mark.unit
def test_get_bootstrap_servers_from_argument():
    """Тест на получение bootstrap_servers из передаваемого аргумента в функцию"""

    assert get_bootstrap_servers(bootstrap_servers="kafka:9093") == "kafka:9093"


@pytest.mark.unit
def test_get_bootstrap_servers_from_env():
    """Тест на получение bootstrap_servers из env"""

    with patch.dict("os.environ", {"KAFKA_BOOTSTRAP_SERVERS": "kafka:9092"}):
        assert get_bootstrap_servers() == "kafka:9092"


@pytest.mark.unit
def test_get_bootstrap_servers_default():
    """Тест, когда bootstrap_servers нет в env"""

    with patch.dict("os.environ", {}):
        assert get_bootstrap_servers() == "localhost:9092"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_wait_kafka_success():
    """Тест на успешное соединение с kafka с переданным аргументом bootstrap_servers"""

    mock_admin = AsyncMock()

    with patch(
        "streaming.plugins.kafka_utils.AIOKafkaAdminClient", return_value=mock_admin
    ):
        await wait_kafka(bootstrap_servers="localhost:9092")
        mock_admin.start.assert_awaited_once()
        mock_admin.close.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_wait_kafka_timeout():
    """Тест на превышение общего времени установить соединение с kafka"""

    mock_admin = AsyncMock()
    mock_admin.start = AsyncMock(side_effect=Exception("Connection refused"))

    with patch(
        "streaming.plugins.kafka_utils.AIOKafkaAdminClient", return_value=mock_admin
    ):
        with pytest.raises(TimeoutError, match="за 1 секунд"):
            await wait_kafka(
                bootstrap_servers="localhost:9092",
                timeout=1,
                max_retries=10,
                retry_interval=1,
            )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_wait_kafka_max_retries():
    """Тест на превышение попыток установить соединение с kafka"""

    mock_admin = AsyncMock()
    mock_admin.start = AsyncMock(side_effect=Exception("Connection refused"))

    with patch(
        "streaming.plugins.kafka_utils.AIOKafkaAdminClient", return_value=mock_admin
    ):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(RuntimeError, match="за 3 попыток"):
                await wait_kafka(
                    bootstrap_servers="localhost:9092", max_retries=3, retry_interval=1
                )


@pytest.mark.unit
def test_create_producer():
    """Тест, что продюсер создаётся с переданным аргументом bootstrap_servers"""

    mock_producer = MagicMock()
    with patch(
        "streaming.plugins.kafka_utils.AIOKafkaProducer", return_value=mock_producer
    ) as mock_producer_cls:
        producer = create_producer(bootstrap_servers="localhost:9092")
        mock_producer_cls.assert_called_once_with(
            bootstrap_servers="localhost:9092",
            value_serializer=mock_producer_cls.call_args[1]["value_serializer"],
            linger_ms=200,
            max_batch_size=1048576,
            compression_type="gzip",
        )
        assert producer == mock_producer


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_topic_success():
    """Тест на успешное создание топика"""

    mock_admin = AsyncMock()
    mock_admin.create_topics = AsyncMock()

    with patch(
        "streaming.plugins.kafka_utils.AIOKafkaAdminClient", return_value=mock_admin
    ) as mock_admin_cls:
        with patch("streaming.plugins.kafka_utils.NewTopic") as mock_new_topic:
            await create_topic(topic_name="topic", bootstrap_servers="localhost:9092")

            mock_admin_cls.assert_called_once_with(
                bootstrap_servers="localhost:9092", client_id="create_topic"
            )
            mock_admin.start.assert_awaited_once()
            mock_admin.create_topics.assert_awaited_once_with(
                [mock_new_topic.return_value]
            )
            mock_admin.close.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_topic_already_exists():
    """Тест на создание топика, если топик уже существует"""

    mock_admin = AsyncMock()
    mock_admin.start = AsyncMock()
    mock_admin.create_topics = AsyncMock(side_effect=TopicAlreadyExistsError("exists"))
    mock_admin.close = AsyncMock()

    with patch(
        "streaming.plugins.kafka_utils.AIOKafkaAdminClient", return_value=mock_admin
    ):
        with patch("time.sleep", return_value=None):
            await create_topic(topic_name="topic", bootstrap_servers=None)
            mock_admin.create_topics.assert_awaited_once()
            mock_admin.close.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_create_topic_error():
    """Тест на ошибку при создании топика"""

    mock_admin = AsyncMock()
    mock_admin.start = AsyncMock()
    mock_admin.create_topics = AsyncMock(side_effect=Exception("Kafka error"))
    mock_admin.close = AsyncMock()

    with patch(
        "streaming.plugins.kafka_utils.AIOKafkaAdminClient", return_value=mock_admin
    ):
        with pytest.raises(Exception, match="Kafka error"):
            await create_topic("topic", "localhost:9092")
        mock_admin.close.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exists_topic_found():
    """Тест на найденный топик"""

    mock_admin = AsyncMock()
    mock_admin.list_topics = AsyncMock(return_value=["topic", "other_topic"])

    with patch(
        "streaming.plugins.kafka_utils.AIOKafkaAdminClient", return_value=mock_admin
    ) as mock_admin_cls:
        result = await exists_topic("topic", "localhost:9092")
        assert result is True
        mock_admin_cls.assert_called_once_with(
            bootstrap_servers="localhost:9092", client_id="check_topic"
        )
        mock_admin.start.assert_awaited_once()
        mock_admin.list_topics.assert_awaited_once()
        mock_admin.close.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exists_topic_not_found():
    """Тест на не найденный топик"""

    mock_admin = AsyncMock()
    mock_admin.list_topics = AsyncMock(return_value={"other_topic"})

    with patch(
        "streaming.plugins.kafka_utils.AIOKafkaAdminClient", return_value=mock_admin
    ) as mock_admin_cls:
        result = await exists_topic("topic", "localhost:9092")
        assert result is False
        mock_admin_cls.assert_called_once_with(
            bootstrap_servers="localhost:9092", client_id="check_topic"
        )
        mock_admin.start.assert_awaited_once()
        mock_admin.list_topics.assert_awaited_once()
        mock_admin.close.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_exists_topic_error():
    """Тест на ошибку при поиске топика"""

    mock_admin = AsyncMock()
    mock_admin.start = AsyncMock()
    mock_admin.list_topics = AsyncMock(side_effect=Exception("Kafka error"))
    mock_admin.close = AsyncMock()

    with patch(
        "streaming.plugins.kafka_utils.AIOKafkaAdminClient", return_value=mock_admin
    ):
        with pytest.raises(Exception, match="Kafka error"):
            await exists_topic("topic", "localhost:9092")
        mock_admin.close.assert_awaited_once()
