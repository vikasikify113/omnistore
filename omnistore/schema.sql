-- PostgreSQL is the source of truth: products, stock, customers, carts, orders.
-- Plus two small plumbing tables: `outbox` (reliable event publishing) and `pipeline_log` (live demo view).

CREATE TABLE IF NOT EXISTS products (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    category    TEXT NOT NULL,
    price       NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    emoji       TEXT NOT NULL DEFAULT '📦'
);

-- Stock per product per city. The CHECK is a last line of defence: stock can never go negative.
-- `version` increases on every change; the search index uses it to ignore out-of-order updates.
CREATE TABLE IF NOT EXISTS inventory (
    product_id  INT  NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    city        TEXT NOT NULL,
    stock       INT  NOT NULL CHECK (stock >= 0),
    version     BIGINT NOT NULL DEFAULT 1,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, city)
);
CREATE INDEX IF NOT EXISTS inventory_city_idx ON inventory (city);

CREATE TABLE IF NOT EXISTS customers (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS carts (
    id         UUID PRIMARY KEY,
    status     TEXT NOT NULL DEFAULT 'open',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cart_items (
    cart_id    UUID NOT NULL REFERENCES carts(id) ON DELETE CASCADE,
    product_id INT  NOT NULL,
    city       TEXT NOT NULL,
    qty        INT  NOT NULL CHECK (qty > 0),
    PRIMARY KEY (cart_id, product_id, city),
    FOREIGN KEY (product_id, city) REFERENCES inventory (product_id, city) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS orders (
    id           SERIAL PRIMARY KEY,
    customer_id  INT NOT NULL REFERENCES customers(id),
    status       TEXT NOT NULL DEFAULT 'placed',   -- placed -> confirmed (by the inventory consumer)
    total        NUMERIC(12,2) NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id   INT  NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id INT  NOT NULL,
    city       TEXT NOT NULL,
    name       TEXT NOT NULL,
    qty        INT  NOT NULL CHECK (qty > 0),
    unit_price NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (order_id, product_id, city)
);

-- Transactional outbox: an event row is written in the SAME transaction as the order/stock change,
-- then relayed to Kafka. If Kafka is down, orders still succeed and events are delivered later.
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT  NOT NULL,
    key          TEXT  NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts     INT NOT NULL DEFAULT 0,
    last_error   TEXT
);
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS notifications (
    id         SERIAL PRIMARY KEY,
    order_id   INT  NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    channel    TEXT NOT NULL,
    recipient  TEXT NOT NULL,
    subject    TEXT NOT NULL,
    body       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (order_id, channel)       -- makes the notification consumer idempotent
);

-- What the "Live pipeline" panel in the UI shows: every stage writes one row.
CREATE TABLE IF NOT EXISTS pipeline_log (
    id         BIGSERIAL PRIMARY KEY,
    ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
    service    TEXT NOT NULL,   -- api | kafka | inventory-consumer | notification-consumer | search-indexer
    event_type TEXT NOT NULL,
    ref        TEXT NOT NULL DEFAULT '',
    action     TEXT NOT NULL
);
