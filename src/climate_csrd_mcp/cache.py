"""
SQLite Cache Layer — Aggressively caches climate data by location key.

Key design:
- climate cache: keyed by `lat_lon_resolution_year` → immutable geodata
- benchmark cache: keyed by `sector_region_year`
- CSRD requirements: keyed by `entity_type_sector`
- KfW funding: keyed by `standort_art_measure`

Cache TTL is intentionally long (30 days for climate data) since
location-based climate projections don't change daily.
"""

import sqlite3
import json
import hashlib
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

DEFAULT_CACHE_PATH = os.environ.get(
    "CLIMATE_CACHE_PATH",
    str(Path(__file__).parent.parent.parent.parent / "climate_cache.db"),
)
CACHE_TTL_DAYS = {
    "climate": 30,       # Klimaprojektionen ändern sich nicht täglich
    "emissions": 7,      # ETS Benchmarks: wöchentlich
    "csrd": 30,          # ESRS-Standards: selten
    "kfw": 14,           # Förderprogramme: 2-wöchentlich
    "weather": 1,        # Aktuelles Wetter: täglich
}


def _make_key(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


class ClimateCache:
    """SQLite-backed cache for climate & CSRD data."""

    def __init__(self, db_path: str = DEFAULT_CACHE_PATH):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            self._init_db()
        return self._conn

    def _init_db(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS climate_cache (
                cache_key TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                category TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                expires_at TEXT NOT NULL
            )
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_category ON climate_cache(category)
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_expires ON climate_cache(expires_at)
        """)
        self._conn.commit()

    def get(self, cache_key: str) -> Optional[dict[str, Any]]:
        """Get cached data if not expired."""
        row = self.conn.execute(
            "SELECT data, expires_at FROM climate_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if row is None:
            return None
        expires = datetime.fromisoformat(row["expires_at"])
        if datetime.utcnow() > expires:
            self.conn.execute(
                "DELETE FROM climate_cache WHERE cache_key = ?", (cache_key,)
            )
            self.conn.commit()
            return None
        return json.loads(row["data"])

    def set(
        self,
        cache_key: str,
        data: dict[str, Any],
        category: str = "climate",
        ttl_days: Optional[int] = None,
    ):
        """Store data in cache with TTL."""
        ttl = ttl_days or CACHE_TTL_DAYS.get(category, 7)
        expires = (datetime.utcnow() + timedelta(days=ttl)).isoformat()
        self.conn.execute(
            """INSERT OR REPLACE INTO climate_cache
               (cache_key, data, category, expires_at)
               VALUES (?, ?, ?, ?)""",
            (cache_key, json.dumps(data), category, expires),
        )
        self.conn.commit()

    def make_key(self, *parts: str) -> str:
        """Generate a deterministic cache key from parts."""
        return _make_key(*parts)

    def clear_expired(self):
        """Remove all expired entries."""
        self.conn.execute(
            "DELETE FROM climate_cache WHERE expires_at < datetime('now')"
        )
        self.conn.commit()

    def clear_category(self, category: str):
        """Clear all entries for a category."""
        self.conn.execute(
            "DELETE FROM climate_cache WHERE category = ?", (category,)
        )
        self.conn.commit()

    def stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total = self.conn.execute("SELECT COUNT(*) FROM climate_cache").fetchone()[0]
        expired = self.conn.execute(
            "SELECT COUNT(*) FROM climate_cache WHERE expires_at < datetime('now')"
        ).fetchone()[0]
        by_category = {
            row["category"]: row["cnt"]
            for row in self.conn.execute(
                "SELECT category, COUNT(*) as cnt FROM climate_cache GROUP BY category"
            ).fetchall()
        }
        return {
            "total_entries": total,
            "expired_entries": expired,
            "active_entries": total - expired,
            "by_category": by_category,
        }

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None


# Module-level singleton
_cache: Optional[ClimateCache] = None


def get_cache() -> ClimateCache:
    global _cache
    if _cache is None:
        _cache = ClimateCache()
    return _cache
