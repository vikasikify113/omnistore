"""Run the Kafka consumers.

    python -m omnistore.consumers all          # all three in one process
    python -m omnistore.consumers inventory    # or one per terminal, which looks great in a live demo
    python -m omnistore.consumers notification
    python -m omnistore.consumers indexer
"""
from __future__ import annotations

import argparse
import json
import signal
import threading
import time
from datetime import datetime

from confluent_kafka import Consumer, KafkaError

from .bus import client_config
from .config import settings
from .db import log_pipeline
from .handlers import handle_indexer, handle_inventory, handle_notification
from .runtime import build_deps

MAX_ATTEMPTS = 3


def specs() -> dict:
    return {
        "inventory": ("omnistore-inventory", [settings.kafka_topic_orders], handle_inventory, "inventory-consumer"),
        "notification": ("omnistore-notification", [settings.kafka_topic_orders], handle_notification, "notification-consumer"),
        "indexer": ("omnistore-search-indexer", [settings.kafka_topic_inventory], handle_indexer, "search-indexer"),
    }


def say(text: str) -> None:
    print(f"{datetime.now():%H:%M:%S} {text}", flush=True)


def process(deps, handler, label: str, msg) -> None:
    meta = {
        "topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
        "key": msg.key().decode() if msg.key() else "",
    }
    ref = f"{meta['topic']}[{meta['partition']}]@{meta['offset']}"
    try:
        event = json.loads(msg.value())
    except (ValueError, TypeError):
        say(f"[{label}] {ref} is not valid JSON, skipped")
        return
    etype = event.get("type", "?")
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            action = handler(deps, event, meta)
            say(f"[{label}] {etype} {ref} -> {action}")
            return
        except Exception as exc:
            say(f"[{label}] {etype} {ref} failed (attempt {attempt}/{MAX_ATTEMPTS}): {exc}")
            if attempt < MAX_ATTEMPTS:
                time.sleep(1.5 * attempt)
    # Give up on this one message so it cannot block everything behind it, but leave a visible trace.
    try:
        with deps.pool.connection() as conn:
            log_pipeline(conn, label, etype, ref, f"FAILED after {MAX_ATTEMPTS} attempts; skipped")
    except Exception:
        pass


def run_consumer(name: str, deps, stop: threading.Event) -> None:
    group, topics, handler, label = specs()[name]
    consumer = Consumer({
        **client_config(settings),
        "group.id": group,
        "enable.auto.commit": False,        # we commit only after the handler succeeded
        "auto.offset.reset": "earliest",    # a consumer that was down catches up on what it missed
        "session.timeout.ms": 30000,
        "client.id": f"omnistore-{name}",
    })
    consumer.subscribe(topics)
    say(f"[{label}] listening on {', '.join(topics)} (group {group})")
    last_error = 0.0
    try:
        while not stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                code = msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    continue
                if time.time() - last_error > 10:  # do not flood the terminal
                    hint = " (does the topic exist? run: python -m omnistore.setup)" if code == KafkaError.UNKNOWN_TOPIC_OR_PART else ""
                    say(f"[{label}] Kafka: {msg.error()}{hint}")
                    last_error = time.time()
                continue
            process(deps, handler, label, msg)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        say(f"[{label}] stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run OmniStore Kafka consumers")
    parser.add_argument("which", choices=["all", *specs().keys()], help="which consumer to run")
    args = parser.parse_args()
    names = list(specs().keys()) if args.which == "all" else [args.which]

    deps = build_deps(pool_size=2 + len(names))
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    threads = [threading.Thread(target=run_consumer, args=(n, deps, stop), name=n, daemon=True) for n in names]
    for t in threads:
        t.start()
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
    except KeyboardInterrupt:
        say("stopping...")
        stop.set()
        for t in threads:
            t.join(timeout=10)


if __name__ == "__main__":
    main()
