"""The HTTP API end to end (real PostgreSQL, fake Kafka and OpenSearch)."""
import pytest
from fastapi.testclient import TestClient

from omnistore import service
from omnistore.api import app
from omnistore.config import settings

from conftest import listing

EARPHONES = "Bluetooth Earphones (Neckband)"


@pytest.fixture
def client(deps):
    deps.search.bulk_index(service.all_listing_docs(deps.pool))
    with TestClient(app) as c:
        yield c


def test_home_page_is_served(client):
    r = client.get("/")
    assert r.status_code == 200 and "OmniStore" in r.text


def test_browse_reads_from_postgresql(client):
    r = client.get("/api/products", params={"city": "Bengaluru", "category": "Electronics"}).json()
    assert r["source"] == "postgresql" and r["total"] > 0
    assert all(i["city"] == "Bengaluru" and i["category"] == "Electronics" for i in r["items"])
    price_filtered = client.get("/api/products", params={"max_price": 500}).json()
    assert all(i["price"] <= 500 for i in price_filtered["items"])


def test_search_is_answered_by_opensearch(client):
    r = client.get("/api/search", params={"q": "Bluetooth", "city": "Bengaluru"}).json()
    assert r["source"] == "opensearch" and r["total"] >= 1


def test_search_failure_is_a_clean_503(client, deps):
    deps.search.search = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("cluster down"))
    r = client.get("/api/search", params={"q": "x"})
    assert r.status_code == 503 and "not available" in r.json()["detail"]


def test_full_shopping_flow(client, deps):
    pid, stock = listing(deps.pool, EARPHONES, "Bengaluru")
    cart_id = client.post("/api/carts").json()["cart_id"]
    cart = client.post(f"/api/carts/{cart_id}/items", json={"product_id": pid, "city": "Bengaluru", "qty": 2}).json()
    assert cart["count"] == 2 and cart["total"] == 2998

    order = client.post("/api/orders", json={"cart_id": cart_id, "customer_name": "Asha", "customer_email": "asha@example.com"})
    assert order.status_code == 201
    oid = order.json()["order_id"]
    # the background task already pushed the event to (fake) Kafka
    assert [e["type"] for _, _, e in deps.bus.sent] == ["order.placed"]
    assert client.get(f"/api/orders/{oid}").json()["status"] == "placed"
    events = client.get("/api/pipeline").json()["events"]
    assert [e["service"] for e in events][:2] == ["api", "kafka"]


def test_sold_out_returns_409_with_a_friendly_message(client, deps):
    pid, _ = listing(deps.pool, "Limited Edition Smartwatch", "Bengaluru")
    carts = []
    for _ in range(2):
        cid = client.post("/api/carts").json()["cart_id"]
        client.post(f"/api/carts/{cid}/items", json={"product_id": pid, "city": "Bengaluru", "qty": 1})
        carts.append(cid)
    body = {"customer_name": "A", "customer_email": "a@example.com"}
    assert client.post("/api/orders", json={**body, "cart_id": carts[0]}).status_code == 201
    r = client.post("/api/orders", json={**body, "cart_id": carts[1]})
    assert r.status_code == 409
    assert r.json()["error"] == "out_of_stock" and "sold out" in r.json()["detail"]


@pytest.mark.parametrize("payload,status", [
    ({"customer_name": "", "customer_email": "a@example.com"}, 422),
    ({"customer_name": "A", "customer_email": "not-an-email"}, 422),
])
def test_order_validation(client, payload, status):
    cart_id = client.post("/api/carts").json()["cart_id"]
    assert client.post("/api/orders", json={"cart_id": cart_id, **payload}).status_code == status


def test_empty_cart_and_unknown_things(client):
    cart_id = client.post("/api/carts").json()["cart_id"]
    r = client.post("/api/orders", json={"cart_id": cart_id, "customer_name": "A", "customer_email": "a@example.com"})
    assert r.status_code == 400
    assert client.get("/api/carts/00000000-0000-0000-0000-000000000000").status_code == 404
    assert client.get("/api/orders/99999").status_code == 404
    assert client.post(f"/api/carts/{cart_id}/items", json={"product_id": 1, "city": "Nowhere", "qty": 1}).status_code == 404
    assert client.post(f"/api/carts/{cart_id}/items", json={"product_id": 1, "city": "Pune", "qty": 0}).status_code == 422


def test_health_reports_all_three_services(client):
    h = client.get("/api/health").json()
    assert h["postgresql"]["ok"] and h["kafka"]["ok"] and h["opensearch"]["ok"]
    assert h["postgresql"]["outbox_pending"] == 0


def test_demo_race_and_restock(client, deps):
    r = client.post("/api/demo/race", json={}).json()
    assert r["winners"] == 1 and r["final_stock"] == 0 and r["oversold"] is False
    pid, before = listing(deps.pool, EARPHONES, "Delhi")
    out = client.post("/api/demo/restock", json={"product_id": pid, "city": "Delhi", "qty": 5}).json()
    assert out["stock"] == before + 5


def test_demo_tools_can_be_switched_off(client, monkeypatch):
    monkeypatch.setattr(settings, "demo_mode", False)
    assert client.post("/api/demo/race", json={}).status_code == 404
    assert client.get("/api/config").json()["demo_mode"] is False
