"""Builds the three service clients once and shares them. Tests swap in fakes with set_deps()."""
from __future__ import annotations

from dataclasses import dataclass

from .bus import KafkaBus
from .config import settings
from .db import make_pool
from .search import SearchClient


@dataclass
class Deps:
    pool: object
    bus: object
    search: object | None
    search_error: str | None = None


_deps: Deps | None = None


def build_deps(pool_size: int | None = None) -> Deps:
    pool = make_pool(settings.database_url, max_size=pool_size or settings.db_pool_max)
    bus = KafkaBus(settings)
    try:
        search, err = SearchClient(settings.opensearch_url, settings.opensearch_index), None
    except Exception as exc:  # e.g. OPENSEARCH_URL missing: the app still starts, search reports the problem
        search, err = None, str(exc)
    return Deps(pool=pool, bus=bus, search=search, search_error=err)


def get_deps() -> Deps:
    global _deps
    if _deps is None:
        _deps = build_deps()
    return _deps


def set_deps(deps: Deps | None) -> None:
    global _deps
    _deps = deps
