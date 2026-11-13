"""Store logic. PostgreSQL is the source of truth; every state change also writes an event to the outbox."""
from __future__ import annotations

import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from psycopg.types.json import Jsonb

from . import events as ev
from .catalog import LAST_ITEM_CITY, LAST_ITEM_NAME
from .config import settings
from .db import log_pipeline


# ---------------------------------------------------------------------------------------------
# Errors the API turns into HTTP responses
# ---------------------------------------------------------------------------------------------
class NotFound(Exception):
    pass


class EmptyCart(Exception):
    pass


class OutOfStock(Exception):
    def __init__(self, product_id: int, city: str, name: str, available: int, requested: int):
        self.product_id, self.city, self.name = product_id, city, name
        self.available, self.requested = available, requested
        super().__init__(f"{name} in {city}: {available} left, {requested} requested")


def _uuid(value: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(value))
    except ValueError:
        raise NotFound("cart not found")


def _num(x):
    return float(x) if isinstance(x, Decimal) else x


# ---------------------------------------------------------------------------------------------
# Browse (PostgreSQL) and facets
# ---------------------------------------------------------------------------------------------
def list_listings(pool, *, city=None, category=None, min_price=None, max_price=None,
                  in_stock=False, page=1, size=24) -> dict:
    where, params = [], []
    if city:
        where.append("i.city = %s"); params.append(city)
    if category:
        where.append("p.category = %s"); params.append(category)
    if min_price is not None:
        where.append("p.price >= %s"); params.append(min_price)
    if max_price is not None:
        where.append("p.price <= %s"); params.append(max_price)
    if in_stock:
        where.append("i.stock > 0")
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    base = f"FROM inventory i JOIN products p ON p.id = i.product_id {clause}"
    page = max(1, page)
    with pool.connection() as conn:
        total = conn.execute(f"SELECT count(*) AS n {base}", params).fetchone()["n"]
        rows = conn.execute(
            f"""SELECT p.id AS product_id, p.name, p.description, p.category, p.price, p.emoji,
                       i.city, i.stock
                {base}
                ORDER BY p.category, p.name, i.city
                LIMIT %s OFFSET %s""",
            params + [size, (page - 1) * size],
        ).fetchall()
    items = [{**r, "price": _num(r["price"]), "listing_id": f"{r['product_id']}:{r['city']}"} for r in rows]
    return {"total": total, "items": items}


def facets(pool) -> dict:
    with pool.connection() as conn:
        cats = [r["category"] for r in conn.execute("SELECT DISTINCT category FROM products ORDER BY 1").fetchall()]
        cities = [r["city"] for r in conn.execute("SELECT DISTINCT city FROM inventory ORDER BY 1").fetchall()]
        pr = conn.execute("SELECT min(price) AS lo, max(price) AS hi FROM products").fetchone()
    return {"categories": cats, "cities": cities, "price_min": _num(pr["lo"] or 0), "price_max": _num(pr["hi"] or 0)}


# ---------------------------------------------------------------------------------------------
# Search documents (used by the indexer consumer and the reindex script)
# ---------------------------------------------------------------------------------------------
def _doc_from_row(r: dict) -> tuple[dict, int]:
    doc = {
        "listing_id": f"{r['product_id']}:{r['city']}",
        "product_id": r["product_id"],
        "city": r["city"],
        "name": r["name"],
        "description": r["description"],
        "category": r["category"],
        "price": _num(r["price"]),
        "stock": r["stock"],
        "emoji": r["emoji"],
        "updated_at": r["updated_at"].isoformat(),
    }
    return doc, int(r["version"])


_DOC_SQL = """SELECT p.id AS product_id, p.name, p.description, p.category, p.price, p.emoji,
                     i.city, i.stock, i.version, i.updated_at
              FROM inventory i JOIN products p ON p.id = i.product_id"""


def fetch_listing_doc(conn, product_id: int, city: str):
    row = conn.execute(_DOC_SQL + " WHERE i.product_id = %s AND i.city = %s", (product_id, city)).fetchone()
    return _doc_from_row(row) if row else (None, None)


def all_listing_docs(pool) -> list[tuple[dict, int]]:
    with pool.connection() as conn:
        return [_doc_from_row(r) for r in conn.execute(_DOC_SQL + " ORDER BY p.id, i.city").fetchall()]


# ---------------------------------------------------------------------------------------------
# Carts
# ---------------------------------------------------------------------------------------------
def create_cart(pool) -> str:
    cart_id = uuid.uuid4()
    with pool.connection() as conn:
        conn.execute("INSERT INTO carts (id) VALUES (%s)", (cart_id,))
    return str(cart_id)


def get_cart(pool, cart_id: str) -> dict:
    cid = _uuid(cart_id)
    with pool.connection() as conn:
        cart = conn.execute("SELECT id, status FROM carts WHERE id = %s", (cid,)).fetchone()
        if not cart:
            raise NotFound("cart not found")
        rows = conn.execute(
            """SELECT ci.product_id, ci.city, ci.qty, p.name, p.emoji, p.price, i.stock
               FROM cart_items ci
               JOIN products p ON p.id = ci.product_id
               JOIN inventory i ON i.product_id = ci.product_id AND i.city = ci.city
               WHERE ci.cart_id = %s ORDER BY p.name, ci.city""",
            (cid,),
        ).fetchall()
    items = [{**r, "price": _num(r["price"]), "line_total": round(_num(r["price"]) * r["qty"], 2)} for r in rows]
    return {
        "id": str(cid),
        "status": cart["status"],
        "items": items,
        "count": sum(i["qty"] for i in items),
        "total": round(sum(i["line_total"] for i in items), 2),
    }


def change_cart_item(pool, cart_id: str, product_id: int, city: str, delta: int) -> None:
    """Add (delta > 0) or remove (delta < 0) units. The cart does not reserve stock; checkout does."""
    cid = _uuid(cart_id)
    with pool.connection() as conn:
        cart = conn.execute("SELECT status FROM carts WHERE id = %s FOR UPDATE", (cid,)).fetchone()
        if not cart or cart["status"] != "open":
            raise NotFound("cart not found or already checked out")
        inv = conn.execute(
            "SELECT i.stock, p.name FROM inventory i JOIN products p ON p.id = i.product_id "
            "WHERE i.product_id = %s AND i.city = %s",
            (product_id, city),
        ).fetchone()
        if not inv:
            raise NotFound("that product is not sold in that city")
        cur = conn.execute(
            "SELECT qty FROM cart_items WHERE cart_id = %s AND product_id = %s AND city = %s",
            (cid, product_id, city),
        ).fetchone()
        new_qty = (cur["qty"] if cur else 0) + delta
        if new_qty <= 0:
            conn.execute(
                "DELETE FROM cart_items WHERE cart_id = %s AND product_id = %s AND city = %s",
                (cid, product_id, city),
            )
        else:
            if new_qty > inv["stock"]:
                raise OutOfStock(product_id, city, inv["name"], inv["stock"], new_qty)
            conn.execute(
                """INSERT INTO cart_items (cart_id, product_id, city, qty) VALUES (%s, %s, %s, %s)
                   ON CONFLICT (cart_id, product_id, city) DO UPDATE SET qty = EXCLUDED.qty""",
                (cid, product_id, city, new_qty),
            )
        conn.execute("UPDATE carts SET updated_at = now() WHERE id = %s", (cid,))


# ---------------------------------------------------------------------------------------------
# Checkout: the one place stock is reserved
# ---------------------------------------------------------------------------------------------
def place_order(pool, cart_id: str, customer_name: str, customer_email: str) -> dict:
    """Everything below happens in ONE PostgreSQL transaction:

      1. reserve stock with a conditional UPDATE (`... WHERE stock >= qty`). Two buyers racing for the
         last unit are serialised by the row lock; the second one's UPDATE matches no row.
      2. save customer, order and order lines
      3. write the `order.placed` event to the outbox
    If any item is short, the whole transaction rolls back: nothing is reserved, nothing is saved.
    """
    cid = _uuid(cart_id)
    name, email = customer_name.strip(), customer_email.strip().lower()
    with pool.connection() as conn:
        cart = conn.execute("SELECT status FROM carts WHERE id = %s FOR UPDATE", (cid,)).fetchone()
        if not cart or cart["status"] != "open":
            raise NotFound("cart not found or already checked out")
        items = conn.execute(
            """SELECT ci.product_id, ci.city, ci.qty, p.name, p.price
               FROM cart_items ci JOIN products p ON p.id = ci.product_id
               WHERE ci.cart_id = %s
               ORDER BY ci.product_id, ci.city""",  # fixed lock order, so two multi-item orders can't deadlock
            (cid,),
        ).fetchall()
        if not items:
            raise EmptyCart()

        for it in items:
            reserved = conn.execute(
                """UPDATE inventory
                      SET stock = stock - %s, version = version + 1, updated_at = now()
                    WHERE product_id = %s AND city = %s AND stock >= %s
                RETURNING stock""",
                (it["qty"], it["product_id"], it["city"], it["qty"]),
            ).fetchone()
            if reserved is None:
                left = conn.execute(
                    "SELECT stock FROM inventory WHERE product_id = %s AND city = %s",
                    (it["product_id"], it["city"]),
                ).fetchone()
                raise OutOfStock(it["product_id"], it["city"], it["name"], left["stock"] if left else 0, it["qty"])

        customer_id = conn.execute(
            """INSERT INTO customers (name, email) VALUES (%s, %s)
               ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name RETURNING id""",
            (name, email),
        ).fetchone()["id"]
        total = sum(Decimal(it["price"]) * it["qty"] for it in items)
        order_id = conn.execute(
            "INSERT INTO orders (customer_id, total) VALUES (%s, %s) RETURNING id", (customer_id, total)
        ).fetchone()["id"]
        for it in items:
            conn.execute(
                "INSERT INTO order_items (order_id, product_id, city, name, qty, unit_price) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (order_id, it["product_id"], it["city"], it["name"], it["qty"], it["price"]),
            )

        line_items = [
            {"product_id": it["product_id"], "city": it["city"], "name": it["name"],
             "qty": it["qty"], "unit_price": _num(it["price"])}
            for it in items
        ]
        event = ev.make_event(ev.ORDER_PLACED, {
            "order_id": order_id,
            "customer": {"name": name, "email": email},
            "total": _num(total),
            "items": line_items,
        })
        conn.execute(
            "INSERT INTO outbox (topic, key, payload) VALUES (%s, %s, %s)",
            (settings.kafka_topic_orders, str(order_id), Jsonb(event)),
        )
        conn.execute("UPDATE carts SET status = 'ordered', updated_at = now() WHERE id = %s", (cid,))
        units = sum(it["qty"] for it in items)
        log_pipeline(conn, "api", ev.ORDER_PLACED, f"order #{order_id}",
                     f"saved in PostgreSQL; reserved {units} unit(s) atomically")
    return {"order_id": order_id, "status": "placed", "total": _num(total), "items": line_items,
            "customer": {"name": name, "email": email}}


def get_order(pool, order_id: int) -> dict:
    with pool.connection() as conn:
        o = conn.execute(
            """SELECT o.id, o.status, o.total, o.created_at, o.confirmed_at, c.name, c.email
               FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.id = %s""",
            (order_id,),
        ).fetchone()
        if not o:
            raise NotFound("order not found")
        items = conn.execute(
            "SELECT product_id, city, name, qty, unit_price FROM order_items WHERE order_id = %s ORDER BY name",
            (order_id,),
        ).fetchall()
        notes = conn.execute(
            "SELECT channel, recipient, subject FROM notifications WHERE order_id = %s", (order_id,)
        ).fetchall()
    return {
        "order_id": o["id"], "status": o["status"], "total": _num(o["total"]),
        "created_at": o["created_at"].isoformat(),
        "confirmed_at": o["confirmed_at"].isoformat() if o["confirmed_at"] else None,
        "customer": {"name": o["name"], "email": o["email"]},
        "items": [{**i, "unit_price": _num(i["unit_price"])} for i in items],
        "notifications": notes,
    }


# ---------------------------------------------------------------------------------------------
# Stock changes outside of checkout (restock / demo reset) -> `inventory.changed` event
# ---------------------------------------------------------------------------------------------
def adjust_stock(pool, product_id: int, city: str, *, add: int | None = None,
                 set_to: int | None = None, reason: str = "restock") -> dict:
    if (add is None) == (set_to is None):
        raise ValueError("pass exactly one of add / set_to")
    if (add is not None and add < 1) or (set_to is not None and set_to < 0):
        raise ValueError("quantity out of range")
    with pool.connection() as conn:
        if set_to is not None:
            row = conn.execute(
                """UPDATE inventory SET stock = %s, version = version + 1, updated_at = now()
                   WHERE product_id = %s AND city = %s RETURNING stock, version""",
                (set_to, product_id, city),
            ).fetchone()
        else:
            row = conn.execute(
                """UPDATE inventory SET stock = stock + %s, version = version + 1, updated_at = now()
                   WHERE product_id = %s AND city = %s RETURNING stock, version""",
                (add, product_id, city),
            ).fetchone()
        if row is None:
            raise NotFound("that product is not sold in that city")
        name = conn.execute("SELECT name FROM products WHERE id = %s", (product_id,)).fetchone()["name"]
        event = ev.make_event(ev.INVENTORY_CHANGED, {
            "product_id": product_id, "city": city, "stock": row["stock"],
            "version": row["version"], "reason": reason,
        })
        conn.execute(
            "INSERT INTO outbox (topic, key, payload) VALUES (%s, %s, %s)",
            (settings.kafka_topic_inventory, ev.inventory_key(product_id, city), Jsonb(event)),
        )
        log_pipeline(conn, "api", ev.INVENTORY_CHANGED, f"{city}",
                     f"{reason}: {name} in {city} now has {row['stock']} in stock")
    return {"product_id": product_id, "city": city, "name": name, "stock": row["stock"]}


# ---------------------------------------------------------------------------------------------
# Outbox relay: PostgreSQL -> Kafka (at-least-once; consumers are idempotent)
# ---------------------------------------------------------------------------------------------
def drain_outbox(pool, bus, limit: int = 50) -> int:
    """Publish unsent events in order. Safe to run from several workers (SKIP LOCKED)."""
    with pool.connection() as conn:
        rows = conn.execute(
            """SELECT id, topic, key, payload, attempts FROM outbox
               WHERE published_at IS NULL ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED""",
            (limit,),
        ).fetchall()
        if not rows:
            return 0
        results = bus.publish_many([(r["topic"], r["key"], json.dumps(r["payload"]).encode()) for r in rows])
        sent = 0
        for r, res in zip(rows, results):
            etype = r["payload"].get("type", "event")
            if res.ok:
                conn.execute(
                    "UPDATE outbox SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = %s",
                    (r["id"],),
                )
                log_pipeline(conn, "kafka", etype, f"{r['topic']}[{res.partition}]@{res.offset}",
                             f"published to Kafka (key {r['key']})")
                sent += 1
            else:
                conn.execute(
                    "UPDATE outbox SET attempts = attempts + 1, last_error = %s WHERE id = %s",
                    (res.error, r["id"]),
                )
                if r["attempts"] == 0:
                    log_pipeline(conn, "kafka", etype, "", "Kafka unreachable: event kept in the outbox, will retry")
        return sent


class OutboxRelay:
    """Background thread in the API process that retries unsent events every few seconds."""

    def __init__(self, pool, bus, interval: float):
        self.pool, self.bus, self.interval = pool, bus, interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="outbox-relay", daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.wait(self.interval):
            try:
                drain_outbox(self.pool, self.bus)
            except Exception as exc:  # keep the relay alive whatever happens
                print(f"[outbox-relay] {exc}", flush=True)


def outbox_pending(pool) -> int:
    with pool.connection() as conn:
        return conn.execute("SELECT count(*) AS n FROM outbox WHERE published_at IS NULL").fetchone()["n"]


# ---------------------------------------------------------------------------------------------
# Live pipeline view
# ---------------------------------------------------------------------------------------------
def recent_pipeline(pool, after_id: int = 0, limit: int = 60) -> list[dict]:
    with pool.connection() as conn:
        if after_id > 0:
            rows = conn.execute(
                "SELECT id, ts, service, event_type, ref, action FROM pipeline_log WHERE id > %s ORDER BY id LIMIT %s",
                (after_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, ts, service, event_type, ref, action FROM pipeline_log ORDER BY id DESC LIMIT %s",
                (limit,),
            ).fetchall()[::-1]
    return [{**r, "ts": r["ts"].isoformat()} for r in rows]


# ---------------------------------------------------------------------------------------------
# Demo: two buyers, one unit, same instant
# ---------------------------------------------------------------------------------------------
def run_race(pool, bus, product_id: int | None = None, city: str | None = None, buyers: int = 2) -> dict:
    buyers = max(2, min(buyers, 6))
    with pool.connection() as conn:
        if product_id is None:
            row = conn.execute("SELECT id FROM products WHERE name = %s", (LAST_ITEM_NAME,)).fetchone()
            if not row:
                raise NotFound("demo product not found; run `python -m omnistore.setup`")
            product_id, city = row["id"], city or LAST_ITEM_CITY
        name = conn.execute("SELECT name FROM products WHERE id = %s", (product_id,)).fetchone()
        if not name or not city:
            raise NotFound("product or city not found")
        name = name["name"]

    adjust_stock(pool, product_id, city, set_to=1, reason="demo reset")  # exactly one unit left
    carts = []
    for _ in range(buyers):
        cid = create_cart(pool)
        change_cart_item(pool, cid, product_id, city, 1)  # every buyer has the item in their cart
        carts.append(cid)

    barrier = threading.Barrier(buyers)

    def buy(i: int) -> dict:
        label = f"Buyer {chr(65 + i)}"
        barrier.wait()  # everyone clicks "Place order" at the same instant
        try:
            order = place_order(pool, carts[i], label, f"buyer{chr(97 + i)}@example.com")
            return {"buyer": label, "result": "won", "order_id": order["order_id"]}
        except OutOfStock:
            return {"buyer": label, "result": "sold_out"}

    with ThreadPoolExecutor(max_workers=buyers) as ex:
        results = list(ex.map(buy, range(buyers)))

    drain_outbox(pool, bus)
    with pool.connection() as conn:
        final = conn.execute(
            "SELECT stock FROM inventory WHERE product_id = %s AND city = %s", (product_id, city)
        ).fetchone()["stock"]
    winners = sum(1 for r in results if r["result"] == "won")
    return {"product": name, "city": city, "buyers": buyers, "results": results,
            "winners": winners, "final_stock": final, "oversold": winners > 1 or final < 0}
