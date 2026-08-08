import pytest

from testcontainers.kafka import KafkaContainer
from testcontainers.redis import RedisContainer
from testcontainers.postgres import PostgresContainer
from sqlalchemy import create_engine

from dwh.settings.setting_pg import PostgresSettings


@pytest.fixture(scope="session")
def kafka_container():
    with KafkaContainer() as kafka:
        yield kafka


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7.2-alpine") as redis:
        yield redis


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16", driver=None) as postgres:
        settings = PostgresSettings.model_validate(
            {
                "HOST": postgres.get_container_host_ip(),
                "PORT": postgres.get_exposed_port(5432),
                "USER": postgres.username,
                "PASS": postgres.password,
                "NAME": postgres.dbname,
            }
        )
        engine = create_engine(url=settings.pg_url)
        yield engine
