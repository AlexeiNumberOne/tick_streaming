import pytest

from streaming.plugins.kafka_utils import KafkaManager

from aiokafka.admin import AIOKafkaAdminClient


@pytest.mark.integration
@pytest.mark.asyncio
async def test_kafka(kafka_container):
    bootstrap = kafka_container.get_bootstrap_server()

    topics = ["test-topic"]

    kafka_manager = KafkaManager(topics=topics, bootstrap_servers=bootstrap)

    await kafka_manager.exists_topics(topics=topics)

    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=bootstrap, client_id="check_topic"
    )
    current_topics = await admin_client.list_topics()
    assert current_topics == topics

    kafka_manager.create_producer()

    await kafka_manager.producer.start()
    await kafka_manager.producer.send(topics[0], value={"test": "message"})
    await kafka_manager.producer.flush()
    await kafka_manager.producer.stop()
