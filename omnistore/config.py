"""All configuration comes from environment variables (see .env.example). No secrets in code."""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return value.strip() if value is not None and value.strip() != "" else default


def _int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


def _bool(name: str, default: bool) -> bool:
    return _env(name, "true" if default else "false").lower() in ("1", "true", "yes", "on")


@dataclass
class Settings:
    # PostgreSQL
    database_url: str
    db_pool_max: int
    # Kafka
    kafka_bootstrap: str
    kafka_security_protocol: str
    kafka_ssl_ca: str
    kafka_ssl_cert: str
    kafka_ssl_key: str
    kafka_sasl_mechanism: str
    kafka_sasl_username: str
    kafka_sasl_password: str
    kafka_topic_orders: str
    kafka_topic_inventory: str
    kafka_partitions: int
    kafka_replication: int  # -1 = broker default
    kafka_message_timeout_ms: int
    # OpenSearch
    opensearch_url: str
    opensearch_index: str
    # App
    demo_mode: bool
    relay_enabled: bool
    relay_interval: float

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=_env("DATABASE_URL"),
            db_pool_max=_int("DB_POOL_MAX", 8),
            kafka_bootstrap=_env("KAFKA_BOOTSTRAP_SERVERS"),
            kafka_security_protocol=_env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper(),
            kafka_ssl_ca=_env("KAFKA_SSL_CA_FILE"),
            kafka_ssl_cert=_env("KAFKA_SSL_CERT_FILE"),
            kafka_ssl_key=_env("KAFKA_SSL_KEY_FILE"),
            kafka_sasl_mechanism=_env("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
            kafka_sasl_username=_env("KAFKA_SASL_USERNAME"),
            kafka_sasl_password=_env("KAFKA_SASL_PASSWORD"),
            kafka_topic_orders=_env("KAFKA_TOPIC_ORDERS", "omnistore.orders"),
            kafka_topic_inventory=_env("KAFKA_TOPIC_INVENTORY", "omnistore.inventory"),
            kafka_partitions=_int("KAFKA_PARTITIONS", 2),
            kafka_replication=_int("KAFKA_REPLICATION_FACTOR", -1),
            kafka_message_timeout_ms=_int("KAFKA_MESSAGE_TIMEOUT_MS", 8000),
            opensearch_url=_env("OPENSEARCH_URL"),
            opensearch_index=_env("OPENSEARCH_INDEX", "omnistore-listings"),
            demo_mode=_bool("DEMO_MODE", True),
            relay_enabled=_bool("RELAY_ENABLED", True),
            relay_interval=float(_env("RELAY_INTERVAL_SECONDS", "2")),
        )


settings = Settings.from_env()
