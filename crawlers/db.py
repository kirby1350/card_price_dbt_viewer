"""PostgreSQL connection adapter for the crawler storage layer.

Provides PgAdapter — a thin wrapper around a psycopg2 connection that
translates DuckDB-dialect SQL to PostgreSQL on the fly, so every crawler
can write to PostgreSQL without changing its internal SQL.

SQL translations handled:
  - ? → %s                              (parameter placeholder style)
  - INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
  - INSERT OR REPLACE → INSERT ... ON CONFLICT (...) DO UPDATE SET ...
  - DOUBLE → DOUBLE PRECISION           (DDL type, CREATE TABLE only)
  - JSON → TEXT                         (DDL type, CREATE TABLE only;
                                         avoids psycopg2 JSON adapter complexity)

Usage::

    import psycopg2
    from crawlers.db import PgAdapter

    conn = PgAdapter(psycopg2.connect("postgresql://user:pass@host/db"))
    # Use exactly like a DuckDB connection:
    conn.execute("CREATE TABLE IF NOT EXISTS ...")
    conn.execute("INSERT OR REPLACE INTO t (a, b) VALUES (?, ?)", [1, 2])
    rows = conn.execute("SELECT * FROM t WHERE a = ?", [1]).fetchall()
    conn.executemany("INSERT OR IGNORE INTO t (a) VALUES (?)", [(1,), (2,)])
    conn.close()
"""

import logging
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-table PRIMARY KEY columns.
# Used to generate ON CONFLICT clauses for INSERT OR REPLACE translations.
# ---------------------------------------------------------------------------
_PK_COLS: dict[str, tuple[str, ...]] = {
    "raw_official_cards":  ("tcg", "card_number", "rarity_code"),
    "zx_sets":             ("set_code",),
    "zx_rarities":         ("rarity_code",),
    "zx_card_name_groups": ("card_name",),
    "yugioh_sets":         ("pid",),
    "vanguard_sets":       ("expansion_id",),
    "weiss_sets":          ("expansion_id",),
    "digimon_sets":        ("category_id",),
    "ua_titles":           ("title_name",),
    "ua_sets":             ("series_id",),
    "yuyutei_sets":          ("game_code", "set_code"),
    "bigweb_cardsets":       ("game_id", "cardset_id"),
    "raw_card_translations": ("tcg", "card_number", "language"),
    "raw_set_translations":  ("tcg", "set_code", "language"),
}

# Matches: INSERT OR (REPLACE|IGNORE) INTO table (col1, col2, ...) VALUES (...)
# The VALUES clause may contain function calls like now(), so we allow one level
# of nested parentheses: [^()]* handles literals/placeholders, (?:\([^()]*\))* handles
# zero-arg or single-arg function calls inside VALUES.
_INSERT_OR_RE = re.compile(
    r"INSERT\s+OR\s+(REPLACE|IGNORE)\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*"
    r"(VALUES\s*\([^()]*(?:\([^()]*\)[^()]*)*\))",
    re.IGNORECASE | re.DOTALL,
)


def _translate_sql(sql: str) -> str:
    """Translate DuckDB-dialect SQL to PostgreSQL-compatible SQL."""

    # 1. INSERT OR REPLACE / INSERT OR IGNORE
    def _replace_insert(m: re.Match) -> str:
        mode = m.group(1).upper()       # REPLACE or IGNORE
        table = m.group(2)
        cols_raw = m.group(3)
        values_clause = m.group(4)      # e.g. "VALUES (?, ?, ?)"
        cols = [c.strip() for c in cols_raw.split(",")]
        insert_head = f"INSERT INTO {table} ({', '.join(cols)}) {values_clause}"

        if mode == "IGNORE":
            return f"{insert_head} ON CONFLICT DO NOTHING"

        # REPLACE → ON CONFLICT DO UPDATE SET non_pk = EXCLUDED.non_pk
        pk = _PK_COLS.get(table, ())
        non_pk = [c for c in cols if c not in pk]
        if pk and non_pk:
            conflict_target = ", ".join(pk)
            update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_pk)
            return (
                f"{insert_head} ON CONFLICT ({conflict_target}) "
                f"DO UPDATE SET {update_clause}"
            )
        # No updatable columns (all are PK), or unknown table → treat as IGNORE
        return f"{insert_head} ON CONFLICT DO NOTHING"

    sql = _INSERT_OR_RE.sub(_replace_insert, sql)

    # 2. Replace ? with %s (parameter placeholders)
    sql = sql.replace("?", "%s")

    # 3. DDL type translations — only inside CREATE TABLE statements
    if re.search(r"\bCREATE\s+TABLE\b", sql, re.IGNORECASE):
        # DOUBLE → DOUBLE PRECISION (only when not already followed by PRECISION)
        sql = re.sub(r"\bDOUBLE\b(?!\s+PRECISION)", "DOUBLE PRECISION", sql)
        # JSON → TEXT (avoids needing psycopg2.extras.Json wrapper for extra fields)
        sql = re.sub(r"\bJSON\b", "TEXT", sql)

    return sql


# ---------------------------------------------------------------------------
# _PgResult — cursor wrapper that mimics DuckDB's execute() return value
# ---------------------------------------------------------------------------

class _PgResult:
    """Wraps a psycopg2 cursor to expose .fetchall() / .fetchone() like DuckDB."""

    __slots__ = ("_cur",)

    def __init__(self, cur):
        self._cur = cur

    def fetchall(self) -> list:
        try:
            return self._cur.fetchall()
        except Exception:
            return []

    def fetchone(self):
        try:
            return self._cur.fetchone()
        except Exception:
            return None


# ---------------------------------------------------------------------------
# PgAdapter — the public API
# ---------------------------------------------------------------------------

class PgAdapter:
    """Adapts a psycopg2 connection to behave like a DuckDB connection.

    Translates DuckDB SQL dialect to PostgreSQL on the fly so that all
    crawler code (storage functions, schema inits, run_full_crawl bodies)
    works against PostgreSQL without modification.
    """

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql: str, params=None) -> _PgResult:
        translated = _translate_sql(sql)
        logger.debug("PgAdapter.execute: %.120s | params=%s", translated, params)
        cur = self._conn.cursor()
        try:
            cur.execute(translated, params or ())
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        return _PgResult(cur)

    def executemany(self, sql: str, params_list) -> None:
        import psycopg2.extras
        translated = _translate_sql(sql)
        logger.debug(
            "PgAdapter.executemany: %.120s | rows=%d", translated, len(params_list)
        )
        cur = self._conn.cursor()
        try:
            psycopg2.extras.execute_batch(cur, translated, params_list)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def close(self) -> None:
        self._conn.commit()
        self._conn.close()
