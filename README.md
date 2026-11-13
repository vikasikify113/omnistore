# OmniStore

An online store for a retail chain moving from physical shops to e-commerce across India, built on three Aiven services. Each one does the job it is best at.

| Service | Its job in OmniStore |
|---|---|
| **Aiven for PostgreSQL** | Source of truth: products, per-city stock, customers, carts, orders. Reserves stock atomically so two buyers can never get the last unit. |
| **Aiven for Apache Kafka** | Event bus. `order.placed` and `inventory.changed` events drive three independent consumers. |
| **Aiven for OpenSearch** | The search bar: typo-tolerant (`bluetoth` finds Bluetooth earphones), filters for category, price and city. |

## Architecture

```mermaid
flowchart LR
    UI[Browser<br/>storefront] -->|browse| API
    UI -->|search| API
    API[FastAPI] -->|"checkout: reserve stock + save order<br/>+ write event (one transaction)"| PG[(PostgreSQL)]
    API -->|search queries| OS[(OpenSearch)]
    PG -->|outbox relay| K1{{"Kafka<br/>omnistore.orders"}}
    K1 --> INV[Inventory consumer<br/>confirms order]
    K1 --> NOTE[Notification consumer<br/>confirmation email]
    INV -->|inventory.changed| K2{{"Kafka<br/>omnistore.inventory"}}
    API -->|restock| K2
    K2 --> IDX[Search indexer]
    INV --> PG
    NOTE --> PG
    IDX -->|reads current state| PG
    IDX -->|re-index listing| OS
```

**What happens when someone places an order**

1. `POST /api/orders` runs one PostgreSQL transaction: reserve stock with `UPDATE ... SET stock = stock - n WHERE stock >= n`, save the order, write an `order.placed` row to the **outbox**.
2. The outbox relay publishes it to Kafka and records the partition and offset.
3. The **inventory consumer** marks the order `confirmed` and emits `inventory.changed` with the new stock.
4. The **notification consumer** records the confirmation email (simulated, no real email is sent).
5. The **search indexer** reads the listing from PostgreSQL and re-indexes it in OpenSearch, so search results show the new stock.

The **Live pipeline** panel on the right of the storefront shows each of these steps as it happens.

### Design decisions worth knowing (good answers for judges)

- **No overselling, by construction.** Stock is reserved inside PostgreSQL at checkout, not afterwards by a Kafka consumer. Concurrent buyers are serialised by the row lock, and a `CHECK (stock >= 0)` backs it up. Items are locked in a fixed order, so two multi-item orders cannot deadlock. A cart never reserves stock; only checkout does.
- **Kafka down? Orders still work.** Events go through a transactional **outbox**: the event row is committed together with the order, then relayed to Kafka (immediately, and retried every 2 s). Nothing is lost.
- **Consumers are idempotent.** Kafka delivers at-least-once. Confirming twice, or sending the "same" email twice, changes nothing (unique constraints, conditional updates, absolute stock values instead of deltas).
- **Out-of-order safe search index.** Each listing carries a PostgreSQL `version`. OpenSearch indexes with `version_type=external_gte`, so a late, older event can never overwrite newer data.
- **One document per product and city**, so the city filter and the stock number are exact.
- **Search and browse are separate paths on purpose.** Empty search box: browse from PostgreSQL. Typing: OpenSearch. The UI labels which one answered, and how fast.

## Run it

You need Python 3.10+.

### 1. Create the three Aiven services

In the [Aiven console](https://console.aiven.io), create one **PostgreSQL**, one **Apache Kafka** and one **OpenSearch** service (the free tier covers all three; use one Aiven account per team, as the free tier allows one service of each type per account). Wait until all three show *Running*.

### 2. Configure

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                    # then edit .env
```

Fill `.env` from each service's **Connection information** page:

- **PostgreSQL**: paste the *Service URI* into `DATABASE_URL`.
- **OpenSearch**: paste the *Service URI* into `OPENSEARCH_URL` (it contains the `avnadmin` user and password).
- **Kafka**: put the host and port into `KAFKA_BOOTSTRAP_SERVERS`. Then use *one* of:
  - **Client certificate** (Aiven's default): download `ca.pem`, `service.cert`, `service.key` into a `certs/` folder and keep `KAFKA_SECURITY_PROTOCOL=SSL`.
  - **SASL**: if your service has SASL enabled, set `KAFKA_SECURITY_PROTOCOL=SASL_SSL` and fill the `KAFKA_SASL_*` lines (use the SASL port).

`.env` and `certs/` are in `.gitignore`. Never commit them.

### 3. Set up and check

```bash
python -m omnistore.setup      # tables, Kafka topics, search index, 43 demo products in 7 cities
python -m omnistore.check      # all three should say [OK]
```

If the topics cannot be created from code, create `omnistore.orders` and `omnistore.inventory` in the Aiven console (Kafka service, *Topics*), then run the setup again.

### 4. Start everything (4 terminals make the best demo)

```bash
uvicorn omnistore.api:app --port 8000                  # terminal 1: storefront + API  -> http://localhost:8000
python -m omnistore.consumers inventory                # terminal 2
python -m omnistore.consumers notification             # terminal 3
python -m omnistore.consumers indexer                  # terminal 4
```

(`python -m omnistore.consumers all` runs the three consumers in one terminal.)

Interactive API docs: http://localhost:8000/docs

### Optional: run it all locally instead

`docker compose up -d`, then use the *local* values listed at the bottom of `.env.example`.

## Put it online (public link)

`start.sh` runs the three consumers and the web app together, so one hosted service is enough
(Render, Railway, Fly.io, any Docker host).

1. Push the repo to GitHub (`.env` and `certs/` are git-ignored, so no secrets go up).
2. On the host, create a **Web Service** from the repo. Start command: `bash start.sh` (or use the included `Dockerfile`).
3. Add the same values as your `.env` as **environment variables** in the host's dashboard (`DATABASE_URL`, `KAFKA_*`, `OPENSEARCH_URL`).
4. Run `python -m omnistore.setup` once from your own machine (it talks to Aiven, not to the host).
5. Open the URL the host gives you. The three dots in the header should be green.

**Kafka on a host:** client certificates are files, which a host does not have. Use **SASL** there
(`KAFKA_SECURITY_PROTOCOL=SASL_SSL` plus the `KAFKA_SASL_*` variables, no files needed), if your Aiven Kafka service has SASL enabled.
Free hosting tiers sleep when idle, so open the page once before you present.

## 3-minute live demo

1. Open http://localhost:8000. The three dots in the header show PostgreSQL, Kafka and OpenSearch are live.
2. **Search**: click *bluetoth earphones* (note the typo). Results come back labelled *from OpenSearch*. Filter by city or price.
3. **Order**: add Bluetooth Earphones in Bengaluru, open the cart, place the order.
4. **Watch the pipeline**: in the right panel, and in the consumer terminals: API saved the order, Kafka published it (with partition and offset), inventory consumer confirmed, notification consumer emailed, search indexer re-indexed. The card's stock number changes and flashes.
5. **Last item**: click **⚡ Race: 2 buyers, 1 last item** in the *Demo tools* box. Exactly one buyer wins, the other sees *sold out*, stock ends at 0.
6. **Resilience** (optional, impressive): stop one consumer, place an order, restart that consumer. It catches up from Kafka.

Set `DEMO_MODE=false` to hide the demo tools and disable their endpoints.

## Tests

```bash
pip install -r requirements-dev.txt
TEST_DATABASE_URL=postgresql://omni:omni@localhost:5432/omnistore_test pytest
```

The tests use a real PostgreSQL and in-memory fakes for Kafka and OpenSearch. **They drop and recreate the OmniStore tables in that database**, so use a throwaway one (`docker compose up -d postgres` creates `omnistore_test`).

They cover, among others: a last-unit race with 12 simultaneous buyers (exactly one wins), 30 buyers sharing 14 units (exactly 14 succeed), rollback of multi-item orders, no deadlocks, duplicate form submits, the outbox surviving a Kafka outage, idempotent consumers, out-of-order index updates, and the exact requests sent to OpenSearch.

### What has and has not been verified

- Verified: all application logic against a real PostgreSQL 16, the HTTP API, the storefront page's JavaScript syntax, Kafka client configuration for SSL and SASL, the request format sent to OpenSearch, and the whole app running with Kafka and OpenSearch down (orders succeed, events wait in the outbox).
- **Not verified here: a live run against real Kafka and OpenSearch servers (including Aiven).** The code uses the standard `confluent-kafka` and `opensearch-py` APIs, but run `python -m omnistore.check` and one test order before you demo, and do a full dress rehearsal.

## Project layout

```
omnistore/
  api.py          FastAPI app and storefront route
  service.py      PostgreSQL logic: browse, carts, atomic checkout, restock, outbox, race demo
  handlers.py     what each Kafka consumer does
  consumers.py    Kafka consumer runner (retries, manual commits)
  bus.py          Kafka producer / admin (Aiven SSL + SASL)
  search.py       OpenSearch mapping, fuzzy query, versioned indexing
  catalog.py      synthetic demo catalog
  schema.sql      database schema
  setup.py        one-time setup;  check.py  connection check
  static/index.html   the storefront (single file, no external dependencies)
tests/            pytest suite
```

## Troubleshooting

- **`check` says Kafka FAIL**: wrong bootstrap address, wrong port for the auth style (SSL and SASL use different ports on Aiven), or wrong certificate paths. The error line says which.
- **Search shows "not available"**: `OPENSEARCH_URL` must include `https://avnadmin:PASSWORD@host:port`. Run `python -m omnistore.setup --reindex` after fixing it.
- **Search results look stale**: is the `indexer` consumer running? Its terminal and the pipeline panel will show it re-indexing.
- **Start over with fresh data**: `python -m omnistore.setup --reset`.
- **Free-tier services stop when idle**: open the Aiven console and make sure all three are *Running* before the demo.
