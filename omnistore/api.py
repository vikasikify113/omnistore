"""HTTP API + the storefront page.   Run:  uvicorn omnistore.api:app --port 8000"""
from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

from . import service
from .catalog import LAST_ITEM_CITY, LAST_ITEM_NAME
from .config import settings
from .runtime import get_deps

STATIC = Path(__file__).parent / "static"
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@asynccontextmanager
async def lifespan(app: FastAPI):
    deps = get_deps()
    relay = None
    if settings.relay_enabled:
        relay = service.OutboxRelay(deps.pool, deps.bus, settings.relay_interval)
        relay.start()
    yield
    if relay:
        relay.stop()


app = FastAPI(title="OmniStore", version="1.0", lifespan=lifespan)


# ---- errors -> HTTP ---------------------------------------------------------------------------
@app.exception_handler(service.NotFound)
async def _not_found(_: Request, exc: service.NotFound):
    return JSONResponse({"detail": str(exc)}, status_code=404)


@app.exception_handler(service.EmptyCart)
async def _empty_cart(_: Request, exc: service.EmptyCart):
    return JSONResponse({"detail": "Your cart is empty."}, status_code=400)


@app.exception_handler(service.OutOfStock)
async def _out_of_stock(_: Request, exc: service.OutOfStock):
    left = f"only {exc.available} left" if exc.available else "sold out"
    return JSONResponse(
        {
            "error": "out_of_stock",
            "detail": f"Sorry, {exc.name} in {exc.city} is {left}.",
            "product_id": exc.product_id, "city": exc.city,
            "available": exc.available, "requested": exc.requested,
        },
        status_code=409,
    )


# ---- request bodies ---------------------------------------------------------------------------
class CartItemIn(BaseModel):
    product_id: int
    city: str = Field(min_length=1, max_length=60)
    qty: int = Field(default=1, description="units to add; use a negative number to remove")

    @field_validator("qty")
    @classmethod
    def _nonzero(cls, v: int) -> int:
        if v == 0 or abs(v) > 50:
            raise ValueError("qty must be between -50 and 50 and not 0")
        return v


class OrderIn(BaseModel):
    cart_id: str
    customer_name: str = Field(min_length=1, max_length=80)
    customer_email: str = Field(max_length=120)

    @field_validator("customer_email")
    @classmethod
    def _email(cls, v: str) -> str:
        if not EMAIL_RE.match(v.strip()):
            raise ValueError("enter a valid email address")
        return v


class RestockIn(BaseModel):
    product_id: int
    city: str
    qty: int = Field(ge=1, le=1000)


class RaceIn(BaseModel):
    product_id: int | None = None
    city: str | None = None
    buyers: int = Field(default=2, ge=2, le=6)


def _require_demo() -> None:
    if not settings.demo_mode:
        raise HTTPException(404, "demo tools are disabled (DEMO_MODE=false)")


# ---- storefront -------------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
def home():
    return FileResponse(STATIC / "index.html")


@app.get("/api/config")
def config():
    return {"demo_mode": settings.demo_mode, "last_item": {"name": LAST_ITEM_NAME, "city": LAST_ITEM_CITY}}


@app.get("/api/health")
def health():
    """Are all three Aiven services reachable? Checked in parallel so a dead one cannot stall the page."""
    deps = get_deps()

    def timed(fn):
        t0 = time.perf_counter()
        try:
            info = fn() or {}
            return {"ok": True, "ms": round((time.perf_counter() - t0) * 1000), **info}
        except Exception as exc:
            return {"ok": False, "ms": round((time.perf_counter() - t0) * 1000), "error": str(exc)[:240]}

    def pg():
        with deps.pool.connection() as conn:
            n = conn.execute("SELECT count(*) AS n FROM inventory").fetchone()["n"]
        return {"listings": n, "outbox_pending": service.outbox_pending(deps.pool)}

    def os_():
        if deps.search is None:
            raise RuntimeError(deps.search_error or "OpenSearch is not configured")
        return deps.search.health()

    with ThreadPoolExecutor(max_workers=3) as ex:
        f_pg, f_kafka, f_os = ex.submit(timed, pg), ex.submit(timed, deps.bus.health), ex.submit(timed, os_)
        return {"postgresql": f_pg.result(), "kafka": f_kafka.result(), "opensearch": f_os.result()}


@app.get("/api/facets")
def get_facets():
    return service.facets(get_deps().pool)


@app.get("/api/products")
def browse(
    category: str | None = None, city: str | None = None,
    min_price: float | None = Query(None, ge=0), max_price: float | None = Query(None, ge=0),
    in_stock: bool = False, page: int = Query(1, ge=1), size: int = Query(24, ge=1, le=60),
):
    """Browsing reads straight from PostgreSQL (the source of truth)."""
    t0 = time.perf_counter()
    out = service.list_listings(get_deps().pool, city=city, category=category, min_price=min_price,
                                max_price=max_price, in_stock=in_stock, page=page, size=size)
    return {**out, "source": "postgresql", "took_ms": round((time.perf_counter() - t0) * 1000)}


@app.get("/api/search")
def search(
    q: str | None = None, category: str | None = None, city: str | None = None,
    min_price: float | None = Query(None, ge=0), max_price: float | None = Query(None, ge=0),
    in_stock: bool = False, page: int = Query(1, ge=1), size: int = Query(24, ge=1, le=60),
):
    """Search is answered by OpenSearch: typo-tolerant, filterable by category, price and city."""
    deps = get_deps()
    if deps.search is None:
        raise HTTPException(503, f"Search is not available: {deps.search_error}")
    try:
        out = deps.search.search(q, category=category, city=city, min_price=min_price,
                                 max_price=max_price, in_stock=in_stock, page=page, size=size)
    except Exception as exc:
        raise HTTPException(503, f"Search is not available: {str(exc)[:200]}")
    return {**out, "source": "opensearch"}


# ---- cart -------------------------------------------------------------------------------------
@app.post("/api/carts", status_code=201)
def new_cart():
    return {"cart_id": service.create_cart(get_deps().pool)}


@app.get("/api/carts/{cart_id}")
def read_cart(cart_id: str):
    return service.get_cart(get_deps().pool, cart_id)


@app.post("/api/carts/{cart_id}/items")
def cart_item(cart_id: str, body: CartItemIn):
    pool = get_deps().pool
    service.change_cart_item(pool, cart_id, body.product_id, body.city, body.qty)
    return service.get_cart(pool, cart_id)


# ---- orders -----------------------------------------------------------------------------------
@app.post("/api/orders", status_code=201)
def checkout(body: OrderIn, background: BackgroundTasks):
    deps = get_deps()
    order = service.place_order(deps.pool, body.cart_id, body.customer_name, body.customer_email)
    # Publish right away, after the response is sent. If Kafka is down the outbox relay retries.
    background.add_task(service.drain_outbox, deps.pool, deps.bus)
    return order


@app.get("/api/orders/{order_id}")
def read_order(order_id: int):
    return service.get_order(get_deps().pool, order_id)


# ---- live pipeline + demo tools ----------------------------------------------------------------
@app.get("/api/pipeline")
def pipeline(after_id: int = Query(0, ge=0), limit: int = Query(60, ge=1, le=200)):
    return {"events": service.recent_pipeline(get_deps().pool, after_id, limit)}


@app.post("/api/demo/restock")
def demo_restock(body: RestockIn, background: BackgroundTasks):
    _require_demo()
    deps = get_deps()
    out = service.adjust_stock(deps.pool, body.product_id, body.city, add=body.qty, reason="restock")
    background.add_task(service.drain_outbox, deps.pool, deps.bus)
    return out


@app.post("/api/demo/race")
def demo_race(body: RaceIn | None = None):
    """Two buyers, one unit, same instant. Exactly one wins and stock never goes below zero."""
    _require_demo()
    body = body or RaceIn()
    deps = get_deps()
    return service.run_race(deps.pool, deps.bus, body.product_id, body.city, body.buyers)
