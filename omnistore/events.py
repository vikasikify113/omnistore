"""Event envelope shared by the API (producer) and the consumers."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

ORDER_PLACED = "order.placed"
INVENTORY_CHANGED = "inventory.changed"


def make_event(event_type: str, data: dict) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "type": event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }


def inventory_key(product_id: int, city: str) -> str:
    """Kafka message key: all events for one listing hit the same partition, so they stay in order."""
    return f"{product_id}:{city}"
