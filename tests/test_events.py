"""Outbox -> Kafka -> consumers -> OpenSearch, with a real PostgreSQL and fake Kafka / OpenSearch."""
from omnistore import handlers, service
from omnistore.config import settings

from conftest import listing, stock_of

EARPHONES = "Bluetooth Earphones (Neckband)"


def make_order(pool, qty=2, city="Bengaluru"):
    pid, _ = listing(pool, EARPHONES, city)
    cid = service.create_cart(pool)
    service.change_cart_item(pool, cid, pid, city, qty)
    return pid, service.place_order(pool, cid, "Asha", "asha@example.com")


def deliver(deps, topic_filter, fn, start=0):
    """Hand every message on a topic to a consumer handler, like Kafka would."""
    n = 0
    for i, (topic, key, event) in enumerate(deps.bus.sent):
        if topic == topic_filter and i >= start:
            fn(deps, event, {"topic": topic, "partition": 0, "offset": i, "key": key})
            n += 1
    return n


def test_outbox_publishes_and_marks_sent(deps):
    make_order(deps.pool)
    assert service.outbox_pending(deps.pool) == 1
    assert service.drain_outbox(deps.pool, deps.bus) == 1
    assert service.outbox_pending(deps.pool) == 0
    topic, key, event = deps.bus.sent[0]
    assert topic == settings.kafka_topic_orders and event["type"] == "order.placed"
    log = service.recent_pipeline(deps.pool)
    assert any(r["service"] == "kafka" and "published" in r["action"] for r in log)


def test_kafka_down_does_not_lose_events_or_block_orders(deps):
    deps.bus.fail = True
    _, order = make_order(deps.pool)  # the order itself still succeeds
    assert order["status"] == "placed"
    assert service.drain_outbox(deps.pool, deps.bus) == 0
    assert service.outbox_pending(deps.pool) == 1
    with deps.pool.connection() as conn:
        row = conn.execute("SELECT attempts, last_error FROM outbox").fetchone()
    assert row["attempts"] == 1 and "broker down" in row["last_error"]
    service.drain_outbox(deps.pool, deps.bus)                 # second failure: no duplicate log spam
    with deps.pool.connection() as conn:
        warnings = conn.execute("SELECT count(*) AS n FROM pipeline_log WHERE action LIKE 'Kafka unreachable%'").fetchone()["n"]
    assert warnings == 1

    deps.bus.fail = False                                      # Kafka comes back
    assert service.drain_outbox(deps.pool, deps.bus) == 1
    assert service.outbox_pending(deps.pool) == 0 and len(deps.bus.sent) == 1


def test_inventory_consumer_confirms_order_and_is_idempotent(deps):
    pid, order = make_order(deps.pool, qty=2)
    service.drain_outbox(deps.pool, deps.bus)
    event = deps.bus.sent[0][2]
    meta = {"topic": settings.kafka_topic_orders, "partition": 0, "offset": 0, "key": "1"}

    handlers.handle_inventory(deps, event, meta)
    handlers.handle_inventory(deps, event, meta)  # Kafka delivered it twice

    got = service.get_order(deps.pool, order["order_id"])
    assert got["status"] == "confirmed" and got["confirmed_at"]
    out = [e for t, k, e in deps.bus.sent if t == settings.kafka_topic_inventory]
    assert len(out) == 2 and all(e["type"] == "inventory.changed" for e in out)
    assert out[0]["data"]["stock"] == stock_of(deps.pool, pid, "Bengaluru")  # absolute value, so duplicates are harmless


def test_inventory_consumer_retries_when_it_cannot_publish(deps):
    _, order = make_order(deps.pool)
    service.drain_outbox(deps.pool, deps.bus)
    event = deps.bus.sent[0][2]
    deps.bus.fail = True
    try:
        handlers.handle_inventory(deps, event, {"topic": "t", "partition": 0, "offset": 0, "key": "1"})
        raised = False
    except RuntimeError:
        raised = True
    assert raised, "the handler must raise so the consumer retries instead of committing the offset"


def test_notification_consumer_sends_once(deps):
    _, order = make_order(deps.pool)
    service.drain_outbox(deps.pool, deps.bus)
    event = deps.bus.sent[0][2]
    meta = {"topic": "t", "partition": 0, "offset": 0, "key": "1"}
    first = handlers.handle_notification(deps, event, meta)
    second = handlers.handle_notification(deps, event, meta)
    assert "sent to asha@example.com" in first and "duplicate" in second
    got = service.get_order(deps.pool, order["order_id"])
    assert len(got["notifications"]) == 1 and got["notifications"][0]["recipient"] == "asha@example.com"


def test_indexer_writes_current_state_and_ignores_stale_versions(deps):
    pid, _ = make_order(deps.pool, qty=3)
    service.drain_outbox(deps.pool, deps.bus)
    deliver(deps, settings.kafka_topic_orders, handlers.handle_inventory)
    deliver(deps, settings.kafka_topic_inventory, handlers.handle_indexer)

    doc, version = deps.search.docs[f"{pid}:Bengaluru"]
    assert doc["stock"] == stock_of(deps.pool, pid, "Bengaluru") and version >= 2

    # an old update arriving late must not overwrite newer data
    stale = dict(doc, stock=999)
    assert deps.search.index_listing(stale, version - 1) == "stale"
    assert deps.search.docs[f"{pid}:Bengaluru"][0]["stock"] == doc["stock"]


def test_restock_flows_to_search_index(deps):
    pid, before = listing(deps.pool, EARPHONES, "Chennai")
    service.adjust_stock(deps.pool, pid, "Chennai", add=10)
    service.drain_outbox(deps.pool, deps.bus)
    deliver(deps, settings.kafka_topic_inventory, handlers.handle_indexer)
    assert deps.search.docs[f"{pid}:Chennai"][0]["stock"] == before + 10


def test_full_chain_order_to_search(deps):
    """order -> outbox -> Kafka -> inventory + notification consumers -> inventory.changed -> indexer."""
    service.drain_outbox(deps.pool, deps.bus)  # nothing yet
    pid, order = make_order(deps.pool, qty=1, city="Mumbai")
    service.drain_outbox(deps.pool, deps.bus)
    deliver(deps, settings.kafka_topic_orders, handlers.handle_inventory)
    deliver(deps, settings.kafka_topic_orders, handlers.handle_notification)
    service.drain_outbox(deps.pool, deps.bus)
    deliver(deps, settings.kafka_topic_inventory, handlers.handle_indexer)

    got = service.get_order(deps.pool, order["order_id"])
    assert got["status"] == "confirmed" and got["notifications"]
    assert deps.search.docs[f"{pid}:Mumbai"][0]["stock"] == stock_of(deps.pool, pid, "Mumbai")
    services = [r["service"] for r in service.recent_pipeline(deps.pool)]
    assert services[0] == "api" and {"kafka", "inventory-consumer", "notification-consumer", "search-indexer"} <= set(services)


def test_reindex_docs_cover_every_listing(pool):
    docs = service.all_listing_docs(pool)
    with pool.connection() as conn:
        n = conn.execute("SELECT count(*) AS n FROM inventory").fetchone()["n"]
    assert len(docs) == n > 100
    assert all(d["listing_id"] == f"{d['product_id']}:{d['city']}" for d, _ in docs)
