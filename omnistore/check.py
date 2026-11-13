"""Quick check that all three Aiven services are reachable and set up.   python -m omnistore.check"""
from __future__ import annotations

import sys

from .bus import KafkaBus
from .config import settings
from .db import make_pool
from .search import SearchClient


def report(name: str, ok: bool, detail: str, hint: str = "") -> bool:
    print(f"  [{'OK' if ok else 'FAIL'}] {name}: {detail}")
    if not ok and hint:
        print(f"         hint: {hint}")
    return ok


def main() -> int:
    print("Checking the three required services...\n")
    results = []

    # PostgreSQL
    pool = None
    try:
        pool = make_pool(settings.database_url, max_size=1)
        with pool.connection() as conn:
            version = conn.execute("SHOW server_version").fetchone()["server_version"]
            tables = conn.execute(
                "SELECT count(*) AS n FROM information_schema.tables WHERE table_name IN ('products','inventory','orders','outbox')"
            ).fetchone()["n"]
            listings = conn.execute("SELECT count(*) AS n FROM inventory").fetchone()["n"] if tables == 4 else 0
        ok = tables == 4
        results.append(report("PostgreSQL", ok, f"v{version}, {listings} listings" if ok else f"v{version}, tables missing",
                              "run: python -m omnistore.setup"))
    except Exception as exc:
        results.append(report("PostgreSQL", False, str(exc).splitlines()[0][:200],
                              "check DATABASE_URL (copy the Service URI from the Aiven console) and that the service is running"))
    finally:
        if pool:
            pool.close()

    # Kafka
    try:
        info = KafkaBus(settings).health()
        md_topics = {settings.kafka_topic_orders, settings.kafka_topic_inventory}
        from confluent_kafka.admin import AdminClient
        from .bus import client_config
        have = set(AdminClient(client_config(settings)).list_topics(timeout=5).topics)
        missing = sorted(md_topics - have)
        results.append(report("Kafka", not missing,
                              f"{info['brokers']} broker(s), topics ready" if not missing else f"missing topics: {', '.join(missing)}",
                              "run: python -m omnistore.setup (or create the topics in the Aiven console)"))
    except Exception as exc:
        results.append(report("Kafka", False, str(exc).splitlines()[0][:200],
                              "check KAFKA_BOOTSTRAP_SERVERS and the certificate paths / SASL login in .env"))

    # OpenSearch
    try:
        client = SearchClient(settings.opensearch_url, settings.opensearch_index)
        info = client.health()
        has_index = "docs" in info
        results.append(report("OpenSearch", has_index,
                              f"v{info['version']}, index '{settings.opensearch_index}' has {info.get('docs')} documents"
                              if has_index else f"v{info['version']}, index '{settings.opensearch_index}' not found",
                              "run: python -m omnistore.setup"))
    except Exception as exc:
        results.append(report("OpenSearch", False, str(exc).splitlines()[0][:200],
                              "check OPENSEARCH_URL (include the avnadmin user and password from the Aiven console)"))

    print()
    if all(results):
        print("All three services are ready. Start the app:  uvicorn omnistore.api:app --port 8000")
        return 0
    print("Fix the items above and run this check again.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
