"""DuckDB storage layer for raw crawled data.

All raw data lands here before dbt transforms it.
Schema is intentionally wide/flexible — normalization happens in dbt.
"""
import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "raw.duckdb"


def get_connection(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def init_schema(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_official_cards (
            tcg             VARCHAR NOT NULL,
            set_code        VARCHAR NOT NULL,
            set_name        VARCHAR,
            card_number     VARCHAR NOT NULL,
            card_name       VARCHAR NOT NULL,
            rarity_code     VARCHAR,
            rarity_name     VARCHAR,
            numbering_scheme VARCHAR,
            card_base_id    VARCHAR,
            image_url       VARCHAR,
            extra           JSON,
            crawled_at      TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (tcg, card_number, rarity_code)
        );

        CREATE TABLE IF NOT EXISTS raw_shop_listings (
            shop            VARCHAR NOT NULL,
            tcg             VARCHAR NOT NULL,
            set_code        VARCHAR,
            card_number_raw VARCHAR NOT NULL,
            card_name_raw   VARCHAR,
            rarity_raw      VARCHAR,
            condition       VARCHAR,
            price           DOUBLE,
            currency        VARCHAR,
            quantity        INTEGER,
            url             VARCHAR,
            crawled_at      TIMESTAMPTZ NOT NULL,
            extra           JSON
        );
    """)
    # Migrate existing tables: add image_url column if not present
    conn.execute("""
        ALTER TABLE raw_official_cards ADD COLUMN IF NOT EXISTS image_url VARCHAR
    """)
    # Backfill from extra JSON for existing rows (best-effort; no-op if function unavailable)
    try:
        conn.execute("""
            UPDATE raw_official_cards
            SET image_url = json_extract_string(extra, '$.image_url')
            WHERE image_url IS NULL AND extra IS NOT NULL
        """)
    except Exception:
        pass
    init_translations_schema(conn)


def insert_official_cards(conn: duckdb.DuckDBPyConnection, records: list[dict]) -> None:
    conn.executemany("""
        INSERT OR REPLACE INTO raw_official_cards
            (tcg, set_code, set_name, card_number, card_name,
             rarity_code, rarity_name, numbering_scheme, card_base_id, image_url, extra)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (r["tcg"], r["set_code"], r["set_name"], r["card_number"], r["card_name"],
         r["rarity_code"], r["rarity_name"], r["numbering_scheme"], r["card_base_id"],
         r.get("image_url", ""), r.get("extra", "{}"))
        for r in records
    ])


def init_zx_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """ZX-specific metadata tables."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS zx_sets (
            set_code       VARCHAR PRIMARY KEY,
            set_name       VARCHAR,
            set_full_value VARCHAR NOT NULL,
            pn_param       VARCHAR NOT NULL,
            total_cards    INTEGER,
            crawled_at     TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS zx_rarities (
            rarity_code VARCHAR PRIMARY KEY,
            rr_param    VARCHAR NOT NULL
        );

        -- Cards that share the same name but appear under different card numbers
        -- (reprints across sets). canonical_number is the lexicographically first
        -- card_number, used as the authoritative card_base_id.
        CREATE TABLE IF NOT EXISTS zx_card_name_groups (
            card_name        VARCHAR PRIMARY KEY,
            canonical_number VARCHAR NOT NULL,
            card_numbers     JSON NOT NULL
        );
    """)


def init_translations_schema(conn) -> None:
    """Translation tables for card and set names in non-source languages."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_card_translations (
            tcg         VARCHAR NOT NULL,
            card_number VARCHAR NOT NULL,
            language    VARCHAR NOT NULL,
            card_name   VARCHAR,
            crawled_at  TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (tcg, card_number, language)
        );

        CREATE TABLE IF NOT EXISTS raw_set_translations (
            tcg        VARCHAR NOT NULL,
            set_code   VARCHAR NOT NULL,
            language   VARCHAR NOT NULL,
            set_name   VARCHAR,
            crawled_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (tcg, set_code, language)
        );
    """)


def insert_card_translations(conn, records: list[dict]) -> None:
    """Upsert card name translations.

    Each record: {"tcg": str, "card_number": str, "language": str, "card_name": str}
    """
    conn.executemany("""
        INSERT OR REPLACE INTO raw_card_translations
            (tcg, card_number, language, card_name)
        VALUES (?, ?, ?, ?)
    """, [
        (r["tcg"], r["card_number"], r["language"], r["card_name"])
        for r in records
    ])


def insert_set_translations(conn, records: list[dict]) -> None:
    """Upsert set name translations.

    Each record: {"tcg": str, "set_code": str, "language": str, "set_name": str}
    """
    conn.executemany("""
        INSERT OR REPLACE INTO raw_set_translations
            (tcg, set_code, language, set_name)
        VALUES (?, ?, ?, ?)
    """, [
        (r["tcg"], r["set_code"], r["language"], r["set_name"])
        for r in records
    ])


def insert_shop_listings(conn: duckdb.DuckDBPyConnection, records: list[dict]) -> None:
    conn.executemany("""
        INSERT INTO raw_shop_listings
            (shop, tcg, set_code, card_number_raw, card_name_raw, rarity_raw,
             condition, price, currency, quantity, url, crawled_at, extra)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (r["shop"], r["tcg"], r.get("set_code"), r["card_number_raw"],
         r.get("card_name_raw"), r.get("rarity_raw"), r.get("condition"),
         r["price"], r["currency"], r.get("quantity"), r["url"],
         r["crawled_at"], r.get("extra", "{}"))
        for r in records
    ])
