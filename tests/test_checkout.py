"""The core promise: stock stays correct, even when many buyers hit the same item at once."""
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from omnistore import service
from omnistore.catalog import LAST_ITEM_CITY, LAST_ITEM_NAME

from conftest import listing, stock_of

EARPHONES = "Bluetooth Earphones (Neckband)"


def cart_with(pool, *items):
    cid = service.create_cart(pool)
    for pid, city, qty in items:
        service.change_cart_item(pool, cid, pid, city, qty)
    return cid


def test_order_reserves_stock_and_writes_outbox(pool):
    pid, before = listing(pool, EARPHONES, "Bengaluru")
    cid = cart_with(pool, (pid, "Bengaluru", 3))
    order = service.place_order(pool, cid, "Asha", "Asha@Example.com")

    assert order["status"] == "placed" and order["total"] == 1499 * 3
    assert stock_of(pool, pid, "Bengaluru") == before - 3
    with pool.connection() as conn:
        ob = conn.execute("SELECT topic, key, payload, published_at FROM outbox").fetchall()
        email = conn.execute("SELECT email FROM customers").fetchone()["email"]
    assert len(ob) == 1 and ob[0]["published_at"] is None
    assert ob[0]["payload"]["type"] == "order.placed" and ob[0]["key"] == str(order["order_id"])
    assert email == "asha@example.com"  # normalised


def test_shortage_rolls_everything_back(pool):
    a, a_stock = listing(pool, EARPHONES, "Bengaluru")
    b, b_stock = listing(pool, LAST_ITEM_NAME, LAST_ITEM_CITY)  # only 1 unit
    cid = cart_with(pool, (a, "Bengaluru", 2), (b, LAST_ITEM_CITY, 1))
    # someone else buys the last watch before this cart checks out
    other = cart_with(pool, (b, LAST_ITEM_CITY, 1))
    service.place_order(pool, other, "Other", "other@example.com")

    with pytest.raises(service.OutOfStock) as exc:
        service.place_order(pool, cid, "Asha", "asha@example.com")
    assert exc.value.available == 0

    assert stock_of(pool, a, "Bengaluru") == a_stock, "the earphones reservation must be rolled back"
    with pool.connection() as conn:
        assert conn.execute("SELECT count(*) AS n FROM orders").fetchone()["n"] == 1  # only the other buyer's
        assert conn.execute("SELECT count(*) AS n FROM outbox").fetchone()["n"] == 1
        assert conn.execute("SELECT status FROM carts WHERE id = %s", (cid,)).fetchone()["status"] == "open"


def test_cart_cannot_hold_more_than_stock(pool):
    pid, stock = listing(pool, LAST_ITEM_NAME, LAST_ITEM_CITY)
    cid = service.create_cart(pool)
    service.change_cart_item(pool, cid, pid, LAST_ITEM_CITY, 1)
    with pytest.raises(service.OutOfStock):
        service.change_cart_item(pool, cid, pid, LAST_ITEM_CITY, 1)  # 2 > 1 in stock


def test_empty_cart_and_unknown_cart(pool):
    with pytest.raises(service.EmptyCart):
        service.place_order(pool, service.create_cart(pool), "A", "a@example.com")
    with pytest.raises(service.NotFound):
        service.place_order(pool, "not-a-uuid", "A", "a@example.com")


def test_last_item_race_has_exactly_one_winner(pool):
    """20 buyers, 1 unit, all released at the same instant."""
    pid, stock = listing(pool, LAST_ITEM_NAME, LAST_ITEM_CITY)
    assert stock == 1
    buyers = 12
    carts = [cart_with(pool, (pid, LAST_ITEM_CITY, 1)) for _ in range(buyers)]
    barrier = threading.Barrier(buyers)

    def buy(i):
        barrier.wait()
        try:
            service.place_order(pool, carts[i], f"B{i}", f"b{i}@example.com")
            return "won"
        except service.OutOfStock:
            return "lost"

    with ThreadPoolExecutor(buyers) as ex:
        results = list(ex.map(buy, range(buyers)))

    assert results.count("won") == 1 and results.count("lost") == buyers - 1
    assert stock_of(pool, pid, LAST_ITEM_CITY) == 0
    with pool.connection() as conn:
        assert conn.execute("SELECT count(*) AS n FROM orders").fetchone()["n"] == 1


def test_many_buyers_never_oversell_a_shared_stock(pool):
    """Stock 14, 30 single-unit buyers: exactly 14 succeed and stock ends at 0."""
    pid, stock = listing(pool, EARPHONES, "Bengaluru")
    buyers = 30
    carts = [cart_with(pool, (pid, "Bengaluru", 1)) for _ in range(buyers)]
    barrier = threading.Barrier(10)

    def buy(i):
        if i < 10:
            barrier.wait()
        try:
            service.place_order(pool, carts[i], f"B{i}", f"b{i}@example.com")
            return 1
        except service.OutOfStock:
            return 0

    with ThreadPoolExecutor(10) as ex:
        won = sum(ex.map(buy, range(buyers)))
    assert won == stock
    assert stock_of(pool, pid, "Bengaluru") == 0


def test_multi_item_orders_in_opposite_order_do_not_deadlock(pool):
    a, _ = listing(pool, EARPHONES, "Mumbai")
    b, _ = listing(pool, EARPHONES, "Delhi")
    service.adjust_stock(pool, a, "Mumbai", set_to=1000)  # plenty, so only a deadlock can fail this test
    service.adjust_stock(pool, b, "Delhi", set_to=1000)
    errors = []

    def worker(first, second, tag):
        try:
            for n in range(15):
                cid = cart_with(pool, first, second)  # insertion order differs between the two workers
                service.place_order(pool, cid, f"{tag}{n}", f"{tag}{n}@example.com")
        except Exception as exc:  # a deadlock would surface here
            errors.append(repr(exc))

    t1 = threading.Thread(target=worker, args=((a, "Mumbai", 1), (b, "Delhi", 1), "x"))
    t2 = threading.Thread(target=worker, args=((b, "Delhi", 1), (a, "Mumbai", 1), "y"))
    t1.start(); t2.start(); t1.join(60); t2.join(60)
    assert not errors, errors


def test_double_submit_of_the_same_cart_creates_one_order(pool):
    pid, _ = listing(pool, EARPHONES, "Bengaluru")
    cid = cart_with(pool, (pid, "Bengaluru", 1))
    barrier = threading.Barrier(2)

    def submit(_):
        barrier.wait()
        try:
            service.place_order(pool, cid, "Asha", "asha@example.com")
            return "ok"
        except service.NotFound:
            return "duplicate"

    with ThreadPoolExecutor(2) as ex:
        results = sorted(ex.map(submit, range(2)))
    assert results == ["duplicate", "ok"]


def test_race_demo_helper(pool, deps):
    out = service.run_race(pool, deps.bus, buyers=3)
    assert out["winners"] == 1 and out["final_stock"] == 0 and not out["oversold"]
    assert sorted(r["result"] for r in out["results"]) == ["sold_out", "sold_out", "won"]
