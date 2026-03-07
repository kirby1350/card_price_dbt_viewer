"""CLI entry point for card_price_dbt_viewer."""
import argparse
import logging
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Card Price DBT Viewer")
    sub = parser.add_subparsers(dest="command", required=True)

    # --- crawl ---
    crawl_p = sub.add_parser("crawl", help="Run a crawler")
    crawl_p.add_argument(
        "target",
        choices=["zx-official", "yugioh-official", "vanguard-official", "weiss-official", "yuyutei-zx", "yuyutei-ygo", "bigweb-zx"],
        help="Which crawler to run",
    )
    crawl_p.add_argument("--delay", type=float, default=1.0, help="Seconds between requests")
    crawl_p.add_argument("--set", dest="set_code", help="Crawl a single set code only")
    crawl_p.add_argument("--debug", action="store_true", help="Enable DEBUG logging")
    crawl_p.add_argument("--db", dest="db", default=None, metavar="PATH",
                         help="DuckDB file to write to (default: data/raw.duckdb)")

    # --- merge ---
    merge_p = sub.add_parser(
        "merge",
        help="Merge per-TCG DuckDB files into the main raw.duckdb. "
             "Useful after running crawlers in parallel with --db.",
    )
    merge_p.add_argument(
        "sources",
        nargs="*",
        metavar="SOURCE",
        help="Source .duckdb files (default: data/raw_*.duckdb excluding raw.duckdb)",
    )
    merge_p.add_argument("--into", dest="target", default=None, metavar="PATH",
                         help="Target DuckDB file (default: data/raw.duckdb)")
    merge_p.add_argument("--debug", action="store_true", help="Enable DEBUG logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.command == "crawl":
        from crawlers.storage import DB_PATH
        db_path = Path(args.db) if args.db else DB_PATH

        if args.target == "zx-official":
            from crawlers.official.zx import ZXOfficialCrawler

            crawler = ZXOfficialCrawler(delay=args.delay)
            if args.set_code:
                import json
                from crawlers.storage import (
                    get_connection, init_schema, init_zx_schema, insert_official_cards,
                )
                sets, rarities = crawler.crawl_metadata()
                conn = get_connection(db_path)
                init_schema(conn)
                init_zx_schema(conn)

                conn.executemany(
                    "INSERT OR IGNORE INTO zx_sets (set_code, set_name, set_full_value, pn_param) VALUES (?, ?, ?, ?)",
                    [(s.set_code, s.set_name, s.set_full_value, s.pn_param) for s in sets],
                )
                conn.executemany(
                    "INSERT OR IGNORE INTO zx_rarities (rarity_code, rr_param) VALUES (?, ?)",
                    rarities,
                )

                batch = []
                for card in crawler.crawl_cards(args.set_code):
                    batch.append({
                        "tcg": card.tcg,
                        "set_code": card.set_code,
                        "set_name": card.set_name,
                        "card_number": card.card_number,
                        "card_name": card.card_name,
                        "rarity_code": card.rarity_code,
                        "rarity_name": card.rarity_name,
                        "numbering_scheme": card.numbering_scheme,
                        "card_base_id": card.card_base_id,
                        "extra": json.dumps(card.extra, ensure_ascii=False),
                    })
                insert_official_cards(conn, batch)
                conn.execute(
                    "UPDATE zx_sets SET total_cards = ?, crawled_at = now() WHERE set_code = ?",
                    [len(batch), args.set_code],
                )
                conn.close()
                print(f"Saved {len(batch)} card editions for set {args.set_code}")
            else:
                crawler.run_full_crawl(db_path=db_path)

        elif args.target == "yugioh-official":
            from crawlers.official.yugioh import YugiohOfficialCrawler, _init_yugioh_schema

            crawler = YugiohOfficialCrawler(delay=args.delay)

            if args.set_code:
                import json
                from crawlers.storage import get_connection, init_schema, insert_official_cards

                conn = get_connection(db_path)
                init_schema(conn)
                _init_yugioh_schema(conn)

                batch = []
                for card in crawler.crawl_cards(args.set_code):
                    batch.append({
                        "tcg": card.tcg,
                        "set_code": card.set_code,
                        "set_name": card.set_name,
                        "card_number": card.card_number,
                        "card_name": card.card_name,
                        "rarity_code": card.rarity_code,
                        "rarity_name": card.rarity_name,
                        "numbering_scheme": card.numbering_scheme,
                        "card_base_id": card.card_base_id,
                        "extra": json.dumps(card.extra, ensure_ascii=False),
                    })
                insert_official_cards(conn, batch)
                conn.close()
                print(f"Saved {len(batch)} card editions for set pid {args.set_code}")
            else:
                crawler.run_full_crawl(db_path=db_path)

        elif args.target == "vanguard-official":
            from crawlers.official.vanguard import VanguardOfficialCrawler, init_vanguard_schema

            crawler = VanguardOfficialCrawler(delay=args.delay)

            if args.set_code:
                import json
                from crawlers.storage import get_connection, init_schema, insert_official_cards

                conn = get_connection(db_path)
                init_schema(conn)
                init_vanguard_schema(conn)

                list(crawler.crawl_sets())

                vg_set = next(s for s in crawler._sets if s.set_code == args.set_code)
                conn.execute(
                    """INSERT OR IGNORE INTO vanguard_sets
                           (expansion_id, set_code, set_name, set_title, category, release_date)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (vg_set.expansion_id, vg_set.set_code, vg_set.set_name,
                     vg_set.set_title, vg_set.category, vg_set.release_date),
                )

                batch = []
                for card in crawler.crawl_cards(args.set_code):
                    batch.append({
                        "tcg": card.tcg,
                        "set_code": card.set_code,
                        "set_name": card.set_name,
                        "card_number": card.card_number,
                        "card_name": card.card_name,
                        "rarity_code": card.rarity_code,
                        "rarity_name": card.rarity_name,
                        "numbering_scheme": card.numbering_scheme,
                        "card_base_id": card.card_base_id,
                        "extra": json.dumps(card.extra, ensure_ascii=False),
                    })
                insert_official_cards(conn, batch)
                conn.execute(
                    "UPDATE vanguard_sets SET total_cards = ?, crawled_at = now() WHERE set_code = ?",
                    [len(batch), args.set_code],
                )
                conn.close()
                print(f"Saved {len(batch)} card editions for set {args.set_code}")
            else:
                crawler.run_full_crawl(db_path=db_path)

        elif args.target == "weiss-official":
            from crawlers.official.weiss import WeissOfficialCrawler, _init_weiss_schema

            crawler = WeissOfficialCrawler(delay=args.delay)

            if args.set_code:
                import json
                from crawlers.storage import get_connection, init_schema, insert_official_cards

                conn = get_connection(db_path)
                init_schema(conn)
                _init_weiss_schema(conn)

                batch = []
                for card in crawler.crawl_cards(args.set_code):
                    batch.append({
                        "tcg": card.tcg,
                        "set_code": card.set_code,
                        "set_name": card.set_name,
                        "card_number": card.card_number,
                        "card_name": card.card_name,
                        "rarity_code": card.rarity_code,
                        "rarity_name": card.rarity_name,
                        "numbering_scheme": card.numbering_scheme,
                        "card_base_id": card.card_base_id,
                        "extra": json.dumps(card.extra, ensure_ascii=False),
                    })
                insert_official_cards(conn, batch)
                conn.close()
                print(f"Saved {len(batch)} card editions for expansion {args.set_code}")
            else:
                crawler.run_full_crawl(db_path=db_path)

        elif args.target == "bigweb-zx":
            from crawlers.shops.bigweb import BigwebShopCrawler

            crawler = BigwebShopCrawler(game_id=151, game_code="zx", tcg="zx", delay=args.delay)

            if args.set_code:
                import json
                from crawlers.storage import get_connection, init_schema, insert_shop_listings
                from crawlers.shops.bigweb import _init_bigweb_schema

                conn = get_connection(db_path)
                init_schema(conn)
                _init_bigweb_schema(conn)

                batch = []
                for listing in crawler.crawl_set(args.set_code.upper()):
                    batch.append({
                        "shop": listing.shop,
                        "tcg": listing.tcg,
                        "set_code": listing.set_code,
                        "card_number_raw": listing.card_number_raw,
                        "card_name_raw": listing.card_name_raw,
                        "rarity_raw": listing.rarity_raw,
                        "condition": listing.condition,
                        "price": listing.price,
                        "currency": listing.currency,
                        "quantity": listing.quantity,
                        "url": listing.url,
                        "crawled_at": listing.crawled_at,
                        "extra": json.dumps(listing.extra, ensure_ascii=False),
                    })
                insert_shop_listings(conn, batch)
                conn.close()
                print(f"Saved {len(batch)} listings for set {args.set_code.upper()}")
            else:
                crawler.run_full_crawl(db_path=db_path)

        elif args.target in ("yuyutei-zx", "yuyutei-ygo"):
            from crawlers.shops.yuyutei import YuyuteiShopCrawler

            game_code, tcg = {
                "yuyutei-zx":  ("zx",  "zx"),
                "yuyutei-ygo": ("ygo", "yugioh"),
            }[args.target]

            crawler = YuyuteiShopCrawler(game_code=game_code, tcg=tcg, delay=args.delay)

            if args.set_code:
                import json
                from crawlers.storage import get_connection, init_schema, insert_shop_listings
                from crawlers.shops.yuyutei import _init_yuyutei_schema

                conn = get_connection(db_path)
                init_schema(conn)
                _init_yuyutei_schema(conn)

                batch = []
                for listing in crawler.crawl_set(args.set_code.upper()):
                    batch.append({
                        "shop": listing.shop,
                        "tcg": listing.tcg,
                        "set_code": listing.set_code,
                        "card_number_raw": listing.card_number_raw,
                        "card_name_raw": listing.card_name_raw,
                        "rarity_raw": listing.rarity_raw,
                        "condition": listing.condition,
                        "price": listing.price,
                        "currency": listing.currency,
                        "quantity": listing.quantity,
                        "url": listing.url,
                        "crawled_at": listing.crawled_at,
                        "extra": json.dumps(listing.extra, ensure_ascii=False),
                    })
                insert_shop_listings(conn, batch)
                conn.close()
                print(f"Saved {len(batch)} listings for set {args.set_code.upper()}")
            else:
                crawler.run_full_crawl(db_path=db_path)

    elif args.command == "merge":
        _run_merge(args)


def _run_merge(args) -> None:
    """Merge per-TCG DuckDB files into the main raw.duckdb using ATTACH."""
    import duckdb
    from crawlers.storage import DB_PATH, get_connection, init_schema, init_zx_schema
    from crawlers.official.yugioh import _init_yugioh_schema
    from crawlers.official.vanguard import init_vanguard_schema
    from crawlers.official.weiss import _init_weiss_schema

    target = Path(args.target) if args.target else DB_PATH

    if args.sources:
        sources = [Path(s) for s in args.sources]
    else:
        # Default: all raw_*.duckdb files in the same directory, excluding the target
        sources = sorted(p for p in target.parent.glob("raw_*.duckdb") if p.resolve() != target.resolve())

    if not sources:
        print("No source files found. Pass source paths explicitly or use --db when crawling.")
        return

    print(f"Merging {len(sources)} source(s) into {target}")

    conn = get_connection(target)
    init_schema(conn)
    init_zx_schema(conn)
    _init_yugioh_schema(conn)
    init_vanguard_schema(conn)
    _init_weiss_schema(conn)

    # TCG-specific tables that may exist in source files
    tcg_tables = ["zx_sets", "zx_rarities", "zx_card_name_groups", "yugioh_sets", "vanguard_sets", "weiss_sets"]

    for i, src in enumerate(sources):
        if not src.exists():
            print(f"  Skipping {src} — file not found")
            continue

        alias = f"_src{i}"
        print(f"  {src.name} ...", end=" ", flush=True)
        conn.execute(f"ATTACH '{src.as_posix()}' AS {alias} (READ_ONLY)")
        try:
            n = conn.execute(
                f"INSERT OR REPLACE INTO raw_official_cards SELECT * FROM {alias}.raw_official_cards"
            ).fetchone()

            for table in tcg_tables:
                has_table = conn.execute(
                    f"SELECT count(*) FROM {alias}.information_schema.tables "
                    f"WHERE table_name = '{table}'"
                ).fetchone()[0]
                if has_table:
                    conn.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM {alias}.{table}")
        finally:
            conn.execute(f"DETACH {alias}")

        count = conn.execute(
            "SELECT changes()"
        ).fetchone()[0]
        print("done")

    total = conn.execute("SELECT count(*) FROM raw_official_cards").fetchone()[0]
    conn.close()
    print(f"Merge complete → {target}  ({total} total official card rows)")


if __name__ == "__main__":
    main()
