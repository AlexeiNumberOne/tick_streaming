import asyncio
import logging

from streaming.plugins.kafka_utils import KafkaManager

from dwh.clickhouse_utils import CHManager
from dwh.models.models_ch import raw_ticks
from dwh.queries.raw.dml import insert_json_rows

TOPICS = ["binance"]
BATCH_SIZE = 1000

logger = logging.getLogger(__name__)


async def main():
    kafka_manager = KafkaManager(topics=TOPICS)
    await kafka_manager.wait_kafka()
    await kafka_manager.exists_topics()
    kafka_manager.create_consumer()
    await kafka_manager.consumer.start()

    logger.info("Consumer started")

    async_ch_manager = CHManager(use_async=True)
    async_ch_manager.register_models(raw_ticks)

    batch: list[dict] = []

    async for msg in kafka_manager.consumer:
        msg_value = msg.value
        batch.append(msg_value)

        if len(batch) >= BATCH_SIZE:
            await async_ch_manager.execute_async(
                insert_json_rows, table=async_ch_manager.models["raw_ticks"], data=batch
            )
            batch = []
            await kafka_manager.consumer.commit()


if __name__ == "__main__":
    asyncio.run(main())
