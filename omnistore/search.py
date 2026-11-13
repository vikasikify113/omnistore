"""OpenSearch adapter: the store's search bar (typo-tolerant, filterable by category, price and city)."""
from __future__ import annotations

from typing import Iterable
from urllib.parse import unquote, urlparse

from opensearchpy import OpenSearch, helpers
from opensearchpy.exceptions import ConflictError, NotFoundError

# One document per (product, city) "listing", so stock and the city filter are exact.
INDEX_BODY = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "text_fold": {"type": "custom", "tokenizer": "standard", "filter": ["lowercase", "asciifolding"]}
            }
        },
    },
    "mappings": {
        "properties": {
            "listing_id": {"type": "keyword"},
            "product_id": {"type": "integer"},
            "city": {"type": "keyword"},
            "category": {"type": "keyword", "fields": {"text": {"type": "text", "analyzer": "text_fold"}}},
            "name": {"type": "text", "analyzer": "text_fold", "fields": {"raw": {"type": "keyword"}}},
            "description": {"type": "text", "analyzer": "text_fold"},
            "price": {"type": "float"},
            "stock": {"type": "integer"},
            "emoji": {"type": "keyword", "index": False},
            "updated_at": {"type": "date"},
        }
    },
}


def build_search_body(
    q: str | None = None,
    *,
    category: str | None = None,
    city: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    in_stock: bool = False,
    page: int = 1,
    size: int = 24,
) -> dict:
    """Pure function (easy to test): turns the shopper's query and filters into an OpenSearch request."""
    q = (q or "").strip()
    filters: list[dict] = []
    if city:
        filters.append({"term": {"city": city}})
    if category:
        filters.append({"term": {"category": category}})
    price: dict = {}
    if min_price is not None:
        price["gte"] = min_price
    if max_price is not None:
        price["lte"] = max_price
    if price:
        filters.append({"range": {"price": price}})
    if in_stock:
        filters.append({"range": {"stock": {"gte": 1}}})

    bool_query: dict = {"filter": filters}
    if q:
        bool_query["must"] = [{
            "bool": {
                "should": [
                    # Typo tolerance: "bluetoth" still finds "Bluetooth" (edit distance scales with word length).
                    {"multi_match": {
                        "query": q,
                        "fields": ["name^3", "category.text^2", "description"],
                        "fuzziness": "AUTO",
                        "prefix_length": 1,
                        "minimum_should_match": "2<75%",
                    }},
                    # Search-as-you-type: "blueto" matches "Bluetooth ...".
                    {"match_phrase_prefix": {"name": {"query": q, "boost": 2}}},
                ],
                "minimum_should_match": 1,
            }
        }]
        sort: list = ["_score", {"name.raw": "asc"}, {"city": "asc"}]
    else:
        bool_query["must"] = [{"match_all": {}}]
        sort = [{"category": "asc"}, {"name.raw": "asc"}, {"city": "asc"}]

    page = max(1, page)
    return {
        "from": (page - 1) * size,
        "size": size,
        "track_total_hits": True,
        "query": {"bool": bool_query},
        "sort": sort,
    }


class SearchClient:
    def __init__(self, url: str, index: str):
        if not url:
            raise RuntimeError("OPENSEARCH_URL is not set. Copy .env.example to .env and fill it in.")
        parsed = urlparse(url)
        secure = parsed.scheme == "https"
        self.index = index
        self.client = OpenSearch(
            hosts=[{"host": parsed.hostname, "port": parsed.port or (443 if secure else 9200)}],
            http_auth=(unquote(parsed.username), unquote(parsed.password or "")) if parsed.username else None,
            use_ssl=secure,
            verify_certs=secure,
            ssl_show_warn=False,
            timeout=20,
            max_retries=2,
            retry_on_timeout=True,
        )

    # ---- admin -------------------------------------------------------------------------------
    def health(self) -> dict:
        info = self.client.info()
        out = {"cluster": info.get("cluster_name"), "version": info.get("version", {}).get("number")}
        if self.client.indices.exists(index=self.index):
            out["docs"] = self.client.count(index=self.index)["count"]
        return out

    def ensure_index(self, reset: bool = False) -> str:
        exists = self.client.indices.exists(index=self.index)
        if exists and reset:
            self.client.indices.delete(index=self.index)
            exists = False
        if not exists:
            self.client.indices.create(index=self.index, body=INDEX_BODY)
            return "created"
        return "exists"

    # ---- writes ------------------------------------------------------------------------------
    def index_listing(self, doc: dict, version: int) -> str:
        """Index one listing. `version` is the Postgres inventory version, so an older update can
        never overwrite a newer one, even if Kafka delivers events out of order or twice."""
        try:
            self.client.index(
                index=self.index,
                id=doc["listing_id"],
                body=doc,
                version=version,
                version_type="external_gte",
                refresh="wait_for",
            )
            return "indexed"
        except ConflictError:
            return "stale"

    def delete_listing(self, listing_id: str) -> None:
        try:
            self.client.delete(index=self.index, id=listing_id, refresh="wait_for")
        except NotFoundError:
            pass

    def bulk_index(self, items: Iterable[tuple[dict, int]]) -> dict:
        actions = (
            {
                "_op_type": "index",
                "_index": self.index,
                "_id": doc["listing_id"],
                "_source": doc,
                "_version": version,
                "_version_type": "external_gte",
            }
            for doc, version in items
        )
        ok, errors = helpers.bulk(self.client, actions, refresh=True, raise_on_error=False)
        return {"indexed": ok, "errors": len(errors) if isinstance(errors, list) else errors}

    # ---- reads -------------------------------------------------------------------------------
    def search(self, q: str | None = None, **filters) -> dict:
        body = build_search_body(q, **filters)
        resp = self.client.search(index=self.index, body=body)
        items = []
        for hit in resp["hits"]["hits"]:
            src = hit["_source"]
            src["score"] = hit.get("_score")
            items.append(src)
        total = resp["hits"]["total"]
        return {
            "total": total["value"] if isinstance(total, dict) else total,
            "took_ms": resp.get("took"),
            "items": items,
        }
