"""One-time setup for all three services.

    python -m omnistore.setup             # create tables, topics and index; seed; index everything
    python -m omnistore.setup --reset     # wipe the tables and the index first (fresh demo data)
    python -m omnistore.setup --reindex   # only rebuild the OpenSearch index from PostgreSQL
"""
from __future__ import annotations

import argparse
import sys

from . import catalog, service
from .bus import KafkaBus
from .config import settings
from .db import apply_schema, drop_all
from .runtime import build_deps


def step(text: str) -> None:
    print(f"\n==> {text}", flush=True)


def first_line(exc: Exception) -> str:
    return (str(exc).splitlines() or [type(exc).__name__])[0][:220]


def reindex(deps, reset: bool) -> bool:
    """Rebuild the OpenSearch index from PostgreSQL. Returns True on success."""
    step("OpenSearch: index")
    if deps.search is None:
        print(f"    skipped: {deps.search_error}")
        return False
    try:
        print(f"    index '{deps.search.index}': {deps.search.ensure_index(reset=reset)}")
        docs = service.all_listing_docs(deps.pool)
        result = deps.search.bulk_index(docs)
        print(f"    indexed {result['indexed']} listings from PostgreSQL ({result['errors']} errors)")
        return result["errors"] == 0
    except Exception as exc:
        print(f"    could not reach OpenSearch: {first_line(exc)}")
        print("    fix OPENSEARCH_URL in .env (it must include the avnadmin user and password), then run this again")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Set up OmniStore on PostgreSQL, Kafka and OpenSearch")
    parser.add_argument("--reset", action="store_true", help="drop tables and the search index first")
    parser.add_argument("--reindex", action="store_true", help="only rebuild the search index")
    args = parser.parse_args()

    deps = build_deps(pool_size=2)

    if args.reindex:
        return 0 if reindex(deps, reset=True) else 1

    step("PostgreSQL: tables")
    if args.reset:
        drop_all(deps.pool)
        print("    dropped existing tables")
    apply_schema(deps.pool)
    print("    schema ready")

    step("PostgreSQL: demo catalog")
    seeded = catalog.seed(deps.pool)
    if seeded["seeded"]:
        print(f"    seeded {seeded['products']} products, {seeded['listings']} city listings")
    else:
        print(f"    already has {seeded['products']} products, {seeded['listings']} listings (use --reset to start over)")

    ok = True
    step("Kafka: topics")
    topics = [settings.kafka_topic_orders, settings.kafka_topic_inventory]
    try:
        for topic, state in KafkaBus(settings).ensure_topics(topics).items():
            print(f"    {topic}: {state}")
            if state.startswith("error"):
                ok = False
                print("    if your Kafka service refuses topic creation, create this topic in the Aiven console")
    except Exception as exc:
        ok = False
        print(f"    could not reach Kafka: {first_line(exc)}")
        print("    fix the KAFKA_* settings in .env (bootstrap address, certificate paths or SASL login), then run this again")

    ok = reindex(deps, reset=args.reset) and ok

    if ok:
        print("\nDone. Next:  python -m omnistore.check")
        return 0
    print("\nSetup finished with problems (see above). PostgreSQL is ready; fix the rest and run this again.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
