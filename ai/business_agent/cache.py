"""Cross-request result cache. The single biggest cost lever in production.

WHY THIS IS SAFE, AND EXACTLY WHERE IT IS NOT.

A mart partition for a CLOSED business date is immutable: EOD has run, the rows are
certified, and nothing rewrites them. Querying the same closed date twice bills twice for a
byte-identical answer. So closed dates are cached indefinitely (bounded by size).

The CURRENT business date is NOT cached beyond a short TTL, because it is still being
written. A cached "today" is how a live dashboard quietly shows an hour-old number, which is
worse than a slow one — the staleness is invisible.

Certification is part of the key. A row that was PROVISIONAL_NRT when cached must not be
served later as though it were CERTIFIED: the tier is a property of the answer, not of the
question.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CACHE_PATH = ROOT / "artifacts" / "validation" / "ai-query-cache.json"

OPEN_DATE_TTL_SECONDS = 300        # 5 min for the day still being written
MAX_ENTRIES = 500


@dataclass
class QueryCache:
    path: Path = CACHE_PATH
    max_entries: int = MAX_ENTRIES
    hits: int = 0
    misses: int = 0
    bytes_saved: int = 0
    _mem: dict = field(default_factory=dict, repr=False)
    _loaded: bool = False

    def _load(self):
        if self._loaded:
            return
        self._loaded = True
        try:
            self._mem = json.loads(self.path.read_text())
        except Exception:                                        # noqa: BLE001
            self._mem = {}

    def _save(self):
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if len(self._mem) > self.max_entries:
                # Evict oldest first. An unbounded cache on a laptop is a slow disk leak.
                for k in sorted(self._mem, key=lambda x: self._mem[x].get("at", 0)
                                )[:len(self._mem) - self.max_entries]:
                    self._mem.pop(k, None)
            self.path.write_text(json.dumps(self._mem))
        except Exception:                                        # noqa: BLE001
            pass                                                 # a cache must never break a query

    @staticmethod
    def _closed(business_date: str | None) -> bool:
        """A date strictly before today is closed and immutable."""
        if not business_date:
            return False
        try:
            return date.fromisoformat(business_date[:10]) < datetime.now(
                timezone.utc).date()
        except ValueError:
            return False

    def key(self, sql: str, business_date: str | None) -> str:
        import hashlib
        return hashlib.sha256(f"{business_date}|{sql}".encode()).hexdigest()[:20]

    def get(self, sql: str, business_date: str | None):
        self._load()
        e = self._mem.get(self.key(sql, business_date))
        if not e:
            self.misses += 1
            return None
        if not self._closed(business_date):
            if time.time() - e.get("at", 0) > OPEN_DATE_TTL_SECONDS:
                self.misses += 1
                return None
        self.hits += 1
        self.bytes_saved += int(e.get("bytes") or 0)
        return e.get("out")

    def put(self, sql: str, business_date: str | None, out: dict):
        self._load()
        self._mem[self.key(sql, business_date)] = {
            "at": time.time(), "out": out,
            "bytes": (out or {}).get("bytes_scanned") or 0,
            "closed": self._closed(business_date)}
        self._save()

    def wrap(self, runner, business_date: str | None):
        """Wrap a runner so every query goes through the cache."""
        def cached(sql: str):
            hit = self.get(sql, business_date)
            if hit is not None:
                return hit
            out = runner(sql)
            self.put(sql, business_date, out)
            return out
        return cached

    def stats(self) -> dict:
        total = self.hits + self.misses
        return {"hits": self.hits, "misses": self.misses,
                "hit_rate": round(self.hits / total, 3) if total else 0.0,
                "athena_bytes_saved": self.bytes_saved,
                "entries": len(self._mem)}
