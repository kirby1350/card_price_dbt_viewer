"""CLI entry point for card_price_dbt_viewer."""
import argparse
import logging


def main() -> None:
    parser = argparse.ArgumentParser(description="Card Price DBT Viewer")
    sub = parser.add_subparsers(dest="command", required=True)

    # --- crawl ---
    crawl_p = sub.add_parser("crawl", help="Run a crawler")
    crawl_p.add_argument(
        "target",
        choices=["zx-official", "yugioh-official", "yuyutei-zx", "yuyutei-ygo", "bigweb-zx"],
        help="Which crawler to run",
    )
    crawl_p.add_argument("--delay", type=float, default=1.0, help="Seconds between requests")
    crawl_p.add_argument("--set", dest="set_code", help="Crawl a single set code only")
    crawl_p.add_argument("--debug", action="store_true", help="Enable DEBUG logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.command == "crawl":
        if args.target == "zx-official":
            from crawlers.official.zx import ZXOfficialCrawler

            crawler = ZXOfficialCrawler(delay=args.delay)
            if args.set_code:
                # Single-set crawl (useful for testing a specific set)
                import json
                from crawlers.storage import (
                    DB_PATH, get_connection, init_schema, init_zx_schema, insert_official_cards,
                )
                sets, rarities = crawler.crawl_metadata()
                conn = get_connection(DB_PATH)
                init_schema(conn)
                init_zx_schema(conn)

                # Store metadata
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
                crawler.run_full_crawl()

        elif args.target == "yugioh-official":
            from crawlers.official.yugioh import YugiohOfficialCrawler, _init_yugioh_schema

            crawler = YugiohOfficialCrawler(delay=args.delay)

            if args.set_code:
                import json
                from crawlers.storage import DB_PATH, get_connection, init_schema, insert_official_cards

                conn = get_connection(DB_PATH)
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
                crawler.run_full_crawl()

        elif args.target == "bigweb-zx":
            from crawlers.shops.bigweb import BigwebShopCrawler

            crawler = BigwebShopCrawler(game_id=151, game_code="zx", tcg="zx", delay=args.delay)

            if args.set_code:
                import json
                from crawlers.storage import DB_PATH, get_connection, init_schema, insert_shop_listings
                from crawlers.shops.bigweb import _init_bigweb_schema

                conn = get_connection(DB_PATH)
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
                crawler.run_full_crawl()

        elif args.target in ("yuyutei-zx", "yuyutei-ygo"):
            from crawlers.shops.yuyutei import YuyuteiShopCrawler

            game_code, tcg = {
                "yuyutei-zx":  ("zx",  "zx"),
                "yuyutei-ygo": ("ygo", "yugioh"),
            }[args.target]

            crawler = YuyuteiShopCrawler(game_code=game_code, tcg=tcg, delay=args.delay)

            if args.set_code:
                import json
                from crawlers.storage import DB_PATH, get_connection, init_schema, insert_shop_listings
                from crawlers.shops.yuyutei import _init_yuyutei_schema

                conn = get_connection(DB_PATH)
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
                crawler.run_full_crawl()


if __name__ == "__main__":
    main()
