"""Apache Kafka adapter (confluent-kafka). Works with Aiven (SSL client cert or SASL) and local Kafka."""
from __future__ import annotations

import threading
from dataclasses import dataclass

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .config import Settings, settings as default_settings


@dataclass
class PublishResult:
    ok: bool
    partition: int | None = None
    offset: int | None = None
    error: str | None = None


def client_config(s: Settings) -> dict:
    """librdkafka client settings for the configured security protocol."""
    if not s.kafka_bootstrap:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is not set. Copy .env.example to .env and fill it in.")
    conf: dict = {"bootstrap.servers": s.kafka_bootstrap}
    proto = s.kafka_security_protocol
    conf["security.protocol"] = proto
    if proto in ("SSL", "SASL_SSL") and s.kafka_ssl_ca:
        conf["ssl.ca.location"] = s.kafka_ssl_ca
    if proto == "SSL":
        # Aiven's default: mutual TLS with the downloaded service.cert / service.key
        if s.kafka_ssl_cert:
            conf["ssl.certificate.location"] = s.kafka_ssl_cert
        if s.kafka_ssl_key:
            conf["ssl.key.location"] = s.kafka_ssl_key
    if proto.startswith("SASL"):
        conf["sasl.mechanisms"] = s.kafka_sasl_mechanism
        conf["sasl.username"] = s.kafka_sasl_username
        conf["sasl.password"] = s.kafka_sasl_password
    return conf


class KafkaBus:
    """Publishes events and reports exactly where each one landed (partition and offset)."""

    def __init__(self, s: Settings | None = None):
        self.s = s or default_settings
        self._producer: Producer | None = None
        self._lock = threading.Lock()

    def _get_producer(self) -> Producer:
        with self._lock:
            if self._producer is None:
                conf = client_config(self.s)
                conf.update({
                    "acks": "all",
                    "enable.idempotence": True,           # no duplicates from producer retries
                    "message.timeout.ms": self.s.kafka_message_timeout_ms,
                    "client.id": "omnistore-api",
                })
                self._producer = Producer(conf)
            return self._producer

    def publish_many(self, messages: list[tuple[str, str, bytes]]) -> list[PublishResult]:
        """messages: [(topic, key, value_bytes)]. Blocks until each is acknowledged or has failed."""
        if not messages:
            return []
        results: list[PublishResult | None] = [None] * len(messages)

        def make_cb(i: int):
            def cb(err, msg):
                if err is not None:
                    results[i] = PublishResult(False, error=str(err))
                else:
                    results[i] = PublishResult(True, msg.partition(), msg.offset())
            return cb

        try:
            producer = self._get_producer()
        except Exception as exc:  # bad config, missing cert files, etc.
            return [PublishResult(False, error=f"producer init failed: {exc}") for _ in messages]

        for i, (topic, key, value) in enumerate(messages):
            try:
                producer.produce(topic, key=key, value=value, on_delivery=make_cb(i))
            except (BufferError, KafkaException) as exc:
                results[i] = PublishResult(False, error=str(exc))
        producer.flush(self.s.kafka_message_timeout_ms / 1000 + 4)
        return [r if r is not None else PublishResult(False, error="delivery timed out") for r in results]

    def health(self) -> dict:
        admin = AdminClient({**client_config(self.s), "socket.timeout.ms": 4000})
        md = admin.list_topics(timeout=4)
        return {"brokers": len(md.brokers), "topics": len(md.topics)}

    def ensure_topics(self, topics: list[str]) -> dict[str, str]:
        """Create topics if missing. Returns {topic: 'created' | 'exists' | 'error: ...'}."""
        admin = AdminClient(client_config(self.s))
        new = [
            NewTopic(t, num_partitions=self.s.kafka_partitions, replication_factor=self.s.kafka_replication)
            for t in topics
        ]
        out: dict[str, str] = {}
        for topic, fut in admin.create_topics(new, operation_timeout=15).items():
            try:
                fut.result()
                out[topic] = "created"
            except KafkaException as exc:
                code = exc.args[0].code() if exc.args else None
                out[topic] = "exists" if code == KafkaError.TOPIC_ALREADY_EXISTS else f"error: {exc}"
        return out
