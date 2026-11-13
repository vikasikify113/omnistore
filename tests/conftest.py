"""Test setup.

Tests use a REAL PostgreSQL (that is where the overselling guarantee lives) and in-memory fakes for Kafka
and OpenSearch. Point TEST_DATABASE_URL at an empty database, for example the one from docker compose:

    TEST_DATABASE_URL=postgresql://omni:omni@localhost:5432/omnistore_test pytest

WARNING: the tests drop and recreate all OmniStore tables in that database.
"""
from __future__ import annotations

import json
import os

import pytest

from omnistore import catalog
from omnistore.bus import PublishResult
from omnistore.config import settings
from omnistore.db import apply_schema, drop_all, make_pool
from omnistore.runtime import Deps, set_deps

DSN = os.getenv("TEST_DATABASE_URL")


class FakeBus:
    def __init__(self):
        self.sent: list[tuple[str, str, dict]] = []
        self.fail = False

    def publish_many(self, messages):
        out = []
        for topic, key, value in messages:
            if self.fail:
                out.append(PublishResult(False, error="broker down"))
            else:
                self.sent.append((topic, key, json.loads(value)))
                out.append(PublishResult(True, partition=0, offset=len(self.sent) - 1))
        return out

    def health(self):
        return {"brokers": 1, "topics": 2}


class FakeSearch:
    """Keeps documents in a dict and mimics external_gte versioning."""

    def __init__(self):
        self.docs: dict[str, tuple[dict, int]] = {}

    def index_listing(self, doc, version):
        cur = self.docs.get(doc["listing_id"])
        if cur and cur[1] > version:
            return "stale"
        self.docs[doc["listing_id"]] = (doc, version)
        return "indexed"

    def delete_listing(self, listing_id):
        self.docs.pop(listing_id, None)

    def bulk_index(self, items):
        n = 0
        for doc, version in items:
            self.index_listing(doc, version)
            n += 1
        return {"indexed": n, "errors": 0}

    def search(self, q=None, **f):
        items = [d for d, _ in self.docs.values()]
        if q:
            items = [d for d in items if q.lower() in d["name"].lower()]
        if f.get("city"):
            items = [d for d in items if d["city"] == f["city"]]
        return {"total": len(items), "took_ms": 1, "items": items}

    def health(self):
        return {"cluster": "fake", "version": "0", "docs": len(self.docs)}


@pytest.fixture
def pool():
    if not DSN:
        pytest.skip("set TEST_DATABASE_URL to run the database tests")
    p = make_pool(DSN, max_size=14, name="tests")
    drop_all(p)
    apply_schema(p)
    catalog.seed(p)
    yield p
    p.close()


@pytest.fixture
def deps(pool):
    d = Deps(pool=pool, bus=FakeBus(), search=FakeSearch())
    set_deps(d)
    yield d
    set_deps(None)


@pytest.fixture(autouse=True)
def _no_background_relay(monkeypatch):
    monkeypatch.setattr(settings, "relay_enabled", False)


def listing(pool, name: str, city: str):
    """(product_id, stock) of a seeded product in a city."""
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT p.id, i.stock FROM products p JOIN inventory i ON i.product_id = p.id "
            "WHERE p.name = %s AND i.city = %s",
            (name, city),
        ).fetchone()
    return row["id"], row["stock"]


def stock_of(pool, product_id: int, city: str) -> int:
    with pool.connection() as conn:
        return conn.execute(
            "SELECT stock FROM inventory WHERE product_id = %s AND city = %s", (product_id, city)
        ).fetchone()["stock"]
