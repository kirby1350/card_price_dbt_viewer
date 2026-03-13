"""Abstract base class for official card list crawlers."""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator


@dataclass
class OfficialCard:
    """Canonical card record from an official card list.

    Fields are TCG-agnostic. TCG-specific fields go into `extra`.
    """
    tcg: str                  # e.g. "yugioh", "zx"
    set_code: str             # e.g. "LOB", "B01"
    set_name: str
    card_number: str          # as printed on card, e.g. "LOB-EN001", "B01-001"
    card_name: str
    rarity_code: str          # canonical rarity code from official source
    rarity_name: str          # canonical rarity name from official source
    # Numbering scheme:
    #   "shared_official"     — same number across rarities, official rarity names (case 1)
    #   "unique_per_rarity"   — different number per rarity (case 2)
    #   "shared_no_official"  — same number across rarities, no official rarity name (case 3)
    numbering_scheme: str
    card_base_id: str | None  # for case 2: groups cards that are logically the same
    image_url: str            # card image URL (empty string if unavailable)
    extra: dict               # TCG-specific fields (e.g. card type, attribute)


class OfficialCrawler(ABC):
    """Base class for official card list crawlers."""

    tcg: str  # must be set by subclass

    @abstractmethod
    def crawl_sets(self) -> Iterator[dict]:
        """Yield raw set/series metadata dicts."""
        ...

    @abstractmethod
    def crawl_cards(self, set_code: str) -> Iterator[OfficialCard]:
        """Yield OfficialCard records for the given set."""
        ...

    def crawl_all(self) -> Iterator[OfficialCard]:
        """Crawl every set and yield all cards."""
        for s in self.crawl_sets():
            yield from self.crawl_cards(s["set_code"])
