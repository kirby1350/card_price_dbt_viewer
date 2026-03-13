"""FastAPI backend for the card price viewer.

Run with:
    uvicorn web.api:app --reload --port 8000
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

load_dotenv()

PG_URL = os.environ["DATABASE_URL"].replace("postgresql+psycopg2://", "postgresql://")

app = FastAPI(title="Card Price Viewer API")

# ---------------------------------------------------------------------------
# Display name maps
# ---------------------------------------------------------------------------

TCG_DISPLAY: dict[str, str] = {
    "unionarena": "Union Arena",
    "yugioh":     "遊戯王 OCG",
    "zx":         "Z/X -Zillions of enemy X-",
    "weiss":      "ヴァイスシュヴァルツ",
    "digimon":    "デジモンカードゲーム",
    "vanguard":   "カードファイト!! ヴァンガード",
}

SHOP_DISPLAY: dict[str, str] = {
    "bigweb":            "Bigweb",
    "yuyutei":           "遊々亭",
    "torecatchi":        "トレカッチ",
    "cardrush":          "カードラッシュ",
    "cardrush-digimon":  "カードラッシュ",
    "cardrush-vanguard": "カードラッシュ",
}

SHOP_COLOR: dict[str, str] = {
    "bigweb":            "#2563eb",
    "yuyutei":           "#d97706",
    "torecatchi":        "#16a34a",
    "cardrush":          "#dc2626",
    "cardrush-digimon":  "#dc2626",
    "cardrush-vanguard": "#dc2626",
}

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


@contextmanager
def get_conn():
    conn = psycopg2.connect(PG_URL)
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------


@app.get("/api/tcgs")
def list_tcgs() -> list[dict]:
    """Return all TCGs that have official card data."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT tcg FROM raw_official_cards ORDER BY tcg"
        )
        return [
            {"tcg": row[0], "display_name": TCG_DISPLAY.get(row[0], row[0])}
            for row in cur.fetchall()
        ]


@app.get("/api/tcgs/{tcg}/sets")
def list_sets(tcg: str) -> list[dict]:
    """Return all sets for a TCG, ordered by set_code."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT set_code, MIN(set_name) AS set_name, COUNT(*) AS card_count
            FROM raw_official_cards
            WHERE tcg = %s
            GROUP BY set_code
            ORDER BY set_code
            """,
            (tcg,),
        )
        rows = cur.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail=f"TCG '{tcg}' not found")
        return [
            {"set_code": r[0], "set_name": r[1] or r[0], "card_count": r[2]}
            for r in rows
        ]


@app.get("/api/tcgs/{tcg}/sets/{set_code}")
def get_set_cards(tcg: str, set_code: str) -> dict[str, Any]:
    """Return all cards in a set with latest prices per shop."""
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Latest listing per (card_number, rarity, shop) by crawled_at.
        # Including rarity_raw in DISTINCT ON keeps both non-parallel (e.g. SR)
        # and parallel (e.g. SR★★) listings for the same card number separate.
        cur.execute(
            """
            WITH latest_listings AS (
                SELECT DISTINCT ON (tcg, upper(trim(card_number_raw)), upper(trim(rarity_raw)), shop)
                    tcg,
                    upper(trim(card_number_raw))  AS card_number_norm,
                    upper(trim(rarity_raw))        AS rarity_norm,
                    shop,
                    price,
                    quantity,
                    url,
                    crawled_at
                FROM raw_shop_listings
                WHERE price > 0 AND tcg = %s
                ORDER BY tcg, upper(trim(card_number_raw)), upper(trim(rarity_raw)), shop, crawled_at DESC
            )
            SELECT
                o.card_number,
                o.card_name,
                o.rarity_code,
                o.rarity_name,
                COALESCE(o.image_url, (o.extra::json)->>'image_url') AS image_url,
                COALESCE(
                    json_agg(
                        json_build_object(
                            'shop',     l.shop,
                            'price',    l.price,
                            'quantity', l.quantity,
                            'url',      l.url
                        ) ORDER BY l.price ASC
                    ) FILTER (WHERE l.shop IS NOT NULL),
                    '[]'::json
                ) AS prices
            FROM raw_official_cards o
            LEFT JOIN latest_listings l
                ON  l.tcg = o.tcg
                AND l.card_number_norm = upper(trim(
                        regexp_replace(o.card_number, '_p[0-9]+$', '', 'gi')
                    ))
                AND l.rarity_norm = upper(trim(
                        regexp_replace(o.rarity_code, '_p[0-9]+$', '', 'gi')
                    ))
            WHERE o.tcg = %s AND upper(trim(o.set_code)) = upper(trim(%s))
            GROUP BY
                o.card_number, o.card_name, o.rarity_code,
                o.rarity_name, o.image_url, o.extra
            ORDER BY o.card_number, o.rarity_code
            """,
            (tcg, tcg, set_code),
        )
        cards = [dict(r) for r in cur.fetchall()]

        if not cards:
            raise HTTPException(
                status_code=404,
                detail=f"Set '{set_code}' not found in TCG '{tcg}'",
            )

        # Annotate each shop entry with display name and color
        for card in cards:
            for price_entry in card.get("prices") or []:
                shop = price_entry.get("shop", "")
                price_entry["display_name"] = SHOP_DISPLAY.get(shop, shop)
                price_entry["color"] = SHOP_COLOR.get(shop, "#6b7280")

        # Last updated: most recent crawl for any card in this set
        cur.execute(
            """
            SELECT MAX(sl.crawled_at)
            FROM raw_shop_listings sl
            JOIN raw_official_cards o
                ON upper(trim(sl.card_number_raw)) = upper(trim(o.card_number))
                AND sl.tcg = o.tcg
            WHERE o.tcg = %s AND upper(trim(o.set_code)) = upper(trim(%s))
            """,
            (tcg, set_code),
        )
        row = cur.fetchone()
        last_updated = row["max"] if row and row["max"] else None

        return {
            "tcg":         tcg,
            "set_code":    set_code,
            "cards":       cards,
            "last_updated": last_updated.isoformat() if last_updated else None,
        }


# ---------------------------------------------------------------------------
# Static files + SPA fallback
# ---------------------------------------------------------------------------

_STATIC = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_STATIC), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(_STATIC, "index.html"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("web.api:app", host="0.0.0.0", port=8000, reload=True)
