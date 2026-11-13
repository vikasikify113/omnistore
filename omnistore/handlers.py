"""What each Kafka consumer does when a message arrives.

Kafka delivers at-least-once, so every handler is idempotent: handling the same message twice is harmless.
Each handler returns a one-line description of what it did (printed in the consumer's terminal and shown
in the UI's live pipeline panel).
"""
from __future__ import annotations

import json

from . import events as ev
from .config import settings
from .db import log_pipeline
from .service import fetch_listing_doc


def _ref(meta: dict) -> str:
    return f"{meta['topic']}[{meta['partition']}]@{meta['offset']}"


# ---- inventory consumer: order.placed -> confirm the order, announce the new stock levels -------
def handle_inventory(deps, event: dict, meta: dict) -> str:
    if event.get("type") != ev.ORDER_PLACED:
        return "ignored"
    data = event["data"]
    order_id = data["order_id"]

    outgoing, summary = [], []
    with deps.pool.connection() as conn:
        # Stock was already reserved atomically at checkout; this step confirms the order (idempotent).
        conn.execute(
            "UPDATE orders SET status = 'confirmed', confirmed_at = now() WHERE id = %s AND status = 'placed'",
            (order_id,),
        )
        for it in data["items"]:
            row = conn.execute(
                "SELECT stock, version FROM inventory WHERE product_id = %s AND city = %s",
                (it["product_id"], it["city"]),
            ).fetchone()
            if row is None:
                continue
            event_out = ev.make_event(ev.INVENTORY_CHANGED, {
                "product_id": it["product_id"], "city": it["city"], "stock": row["stock"],
                "version": row["version"], "reason": "order_placed", "order_id": order_id,
            })
            outgoing.append((settings.kafka_topic_inventory, ev.inventory_key(it["product_id"], it["city"]),
                             json.dumps(event_out).encode()))
            summary.append(f"{it['name']} ({it['city']}) now {row['stock']}")

    results = deps.bus.publish_many(outgoing)
    bad = [r for r in results if not r.ok]
    if bad:
        raise RuntimeError(f"could not publish inventory.changed: {bad[0].error}")  # Kafka redelivers the order

    action = f"order #{order_id} confirmed; stock now: " + "; ".join(summary)
    with deps.pool.connection() as conn:
        log_pipeline(conn, "inventory-consumer", ev.ORDER_PLACED, _ref(meta), action)
    return action


# ---- notification consumer: order.placed -> confirmation email ------------------------------------
def handle_notification(deps, event: dict, meta: dict) -> str:
    if event.get("type") != ev.ORDER_PLACED:
        return "ignored"
    data = event["data"]
    order_id, customer = data["order_id"], data["customer"]
    lines = [f"Hi {customer['name']},", "", "Thanks for shopping with OmniStore. Your order:"]
    for it in data["items"]:
        lines.append(f"  {it['qty']} x {it['name']} ({it['city']}): Rs {it['unit_price'] * it['qty']:,.0f}")
    lines += ["", f"Total: Rs {data['total']:,.0f}"]
    subject = f"Your OmniStore order #{order_id} is confirmed"

    with deps.pool.connection() as conn:
        inserted = conn.execute(
            """INSERT INTO notifications (order_id, channel, recipient, subject, body)
               VALUES (%s, 'email', %s, %s, %s)
               ON CONFLICT (order_id, channel) DO NOTHING RETURNING id""",
            (order_id, customer["email"], subject, "\n".join(lines)),
        ).fetchone()
        action = (f"confirmation email for order #{order_id} sent to {customer['email']} (simulated)"
                  if inserted else f"order #{order_id}: confirmation already sent, skipped duplicate")
        log_pipeline(conn, "notification-consumer", ev.ORDER_PLACED, _ref(meta), action)
    return action


# ---- search indexer: inventory.changed -> refresh the OpenSearch document ----------------------------
def handle_indexer(deps, event: dict, meta: dict) -> str:
    if event.get("type") != ev.INVENTORY_CHANGED:
        return "ignored"
    if deps.search is None:
        raise RuntimeError(deps.search_error or "OpenSearch is not configured")
    data = event["data"]
    pid, city = data["product_id"], data["city"]

    # The event is a nudge; PostgreSQL is the truth, so read the current state from it.
    with deps.pool.connection() as conn:
        doc, version = fetch_listing_doc(conn, pid, city)

    if doc is None:
        deps.search.delete_listing(f"{pid}:{city}")
        action = f"listing {pid} in {city} no longer exists: removed from OpenSearch"
    elif deps.search.index_listing(doc, version) == "indexed":
        action = f"re-indexed {doc['name']} in {city}: stock {doc['stock']} (v{version}) in OpenSearch"
    else:
        action = f"{doc['name']} in {city}: a newer version is already indexed, skipped"

    with deps.pool.connection() as conn:
        log_pipeline(conn, "search-indexer", ev.INVENTORY_CHANGED, _ref(meta), action)
    return action
