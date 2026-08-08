import pytest

from streaming.plugins.kafka_utils import create_topic, exists_topic, create_producer


@pytest.mark.integration
@pytest.mark.asyncio
async def test_kafka(kafka_container):
    bootstrap = kafka_container.get_bootstrap_server()
    assert bootstrap is not None

    topic = "test-topic"

    await create_topic(topic_name=topic, bootstrap_servers=bootstrap)

    assert await exists_topic(topic, bootstrap) is True

    producer = create_producer(bootstrap)

    await producer.start()
    await producer.send(topic, value={"test": "message"})
    await producer.flush()
    await producer.stop()
