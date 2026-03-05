"""Abstract base class for shop price crawlers."""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator


@dataclass
class ShopListing:
    """A single card listing scraped from a shop.

    card_number and rarity_raw are as the shop displays them — they will be
    normalized and matched to card_edition_id in the dbt intermediate layer.
    """
    shop: str                     # shop identifier, e.g. "yuyutei"
    tcg: str                      # e.g. "yugioh", "zx"
    set_code: str | None          # may be None if shop doesn't expose it
    card_number_raw: str          # card number as the shop shows it
    card_name_raw: str
    rarity_raw: str               # rarity string as the shop shows it
    condition: str                # e.g. "NM", "LP", "MP", "HP", "DMG"
    price: float
    currency: str                 # ISO 4217, e.g. "JPY", "USD"
    quantity: int
    url: str
    crawled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    extra: dict = field(default_factory=dict)


class ShopCrawler(ABC):
    """Base class for shop price crawlers."""

    shop: str   # must be set by subclass
    tcg: str    # TCG this shop covers; set to "*" if multi-TCG

    @abstractmethod
    def crawl_set(self, set_code: str) -> Iterator[ShopListing]:
        """Yield listings for a specific set."""
        ...

    @abstractmethod
    def search_card(self, card_number: str) -> Iterator[ShopListing]:
        """Yield listings for a specific card number."""
        ...
