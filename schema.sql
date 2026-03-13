-- PostgreSQL schema for card_price_dbt_viewer
-- Run this once to set up all tables before crawling.
-- JSON fields stored as TEXT (containing valid JSON strings).

-- ---------------------------------------------------------------------------
-- Core tables
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw_official_cards (
    tcg              VARCHAR      NOT NULL,
    set_code         VARCHAR      NOT NULL,
    set_name         VARCHAR,
    card_number      VARCHAR      NOT NULL,
    card_name        VARCHAR      NOT NULL,
    rarity_code      VARCHAR,
    rarity_name      VARCHAR,
    numbering_scheme VARCHAR,
    card_base_id     VARCHAR,
    extra            TEXT,                           -- JSON string
    crawled_at       TIMESTAMPTZ  DEFAULT now(),
    PRIMARY KEY (tcg, card_number, rarity_code)
);

CREATE TABLE IF NOT EXISTS raw_shop_listings (
    shop            VARCHAR      NOT NULL,
    tcg             VARCHAR      NOT NULL,
    set_code        VARCHAR,
    card_number_raw VARCHAR      NOT NULL,
    card_name_raw   VARCHAR,
    rarity_raw      VARCHAR,
    condition       VARCHAR,
    price           DOUBLE PRECISION,
    currency        VARCHAR,
    quantity        INTEGER,
    url             VARCHAR,
    crawled_at      TIMESTAMPTZ  NOT NULL,
    extra           TEXT                             -- JSON string
);

-- ---------------------------------------------------------------------------
-- Z/X metadata
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zx_sets (
    set_code       VARCHAR      PRIMARY KEY,
    set_name       VARCHAR,
    set_full_value VARCHAR      NOT NULL,
    pn_param       VARCHAR      NOT NULL,
    total_cards    INTEGER,
    crawled_at     TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS zx_rarities (
    rarity_code VARCHAR  PRIMARY KEY,
    rr_param    VARCHAR  NOT NULL
);

-- Cards that share the same name but appear under different card numbers (reprints).
-- canonical_number is the lexicographically first card_number.
CREATE TABLE IF NOT EXISTS zx_card_name_groups (
    card_name        VARCHAR  PRIMARY KEY,
    canonical_number VARCHAR  NOT NULL,
    card_numbers     TEXT     NOT NULL               -- JSON array of card number strings
);

-- ---------------------------------------------------------------------------
-- Yu-Gi-Oh metadata
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS yugioh_sets (
    pid          VARCHAR  PRIMARY KEY,
    set_code     VARCHAR,
    set_name     VARCHAR,
    release_date VARCHAR,
    category     VARCHAR,
    total_cards  INTEGER,
    crawled_at   TIMESTAMPTZ
);

-- ---------------------------------------------------------------------------
-- Cardfight!! Vanguard metadata
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS vanguard_sets (
    expansion_id  INTEGER  PRIMARY KEY,
    set_code      VARCHAR  NOT NULL,
    set_name      VARCHAR,
    set_title     VARCHAR,
    category      VARCHAR,
    release_date  VARCHAR,
    total_cards   INTEGER,
    crawled_at    TIMESTAMPTZ
);

-- ---------------------------------------------------------------------------
-- Weiss Schwarz metadata
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS weiss_sets (
    expansion_id  INTEGER  PRIMARY KEY,
    set_code      VARCHAR,
    set_name      VARCHAR,
    total_cards   INTEGER,
    crawled_at    TIMESTAMPTZ
);

-- ---------------------------------------------------------------------------
-- Digimon Card Game metadata
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS digimon_sets (
    category_id  INTEGER  PRIMARY KEY,
    set_code     VARCHAR,
    set_name     VARCHAR,
    total_cards  INTEGER,
    crawled_at   TIMESTAMPTZ
);

-- ---------------------------------------------------------------------------
-- Union Arena metadata
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ua_titles (
    title_name  VARCHAR  PRIMARY KEY,
    crawled_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ua_sets (
    series_id    INTEGER  PRIMARY KEY,
    set_code     VARCHAR,
    set_name     VARCHAR,
    title_name   VARCHAR,
    total_cards  INTEGER,
    crawled_at   TIMESTAMPTZ
);

-- ---------------------------------------------------------------------------
-- Translations
-- ---------------------------------------------------------------------------

-- Translated card names, keyed by (tcg, card_number, language).
-- card_number matches raw_official_cards.card_number exactly.
-- language uses BCP-47 codes: 'en', 'ko', 'zh-TW', etc.
CREATE TABLE IF NOT EXISTS raw_card_translations (
    tcg         VARCHAR      NOT NULL,
    card_number VARCHAR      NOT NULL,
    language    VARCHAR      NOT NULL,
    card_name   VARCHAR,
    crawled_at  TIMESTAMPTZ  DEFAULT now(),
    PRIMARY KEY (tcg, card_number, language)
);

-- Translated set names, keyed by (tcg, set_code, language).
CREATE TABLE IF NOT EXISTS raw_set_translations (
    tcg        VARCHAR      NOT NULL,
    set_code   VARCHAR      NOT NULL,
    language   VARCHAR      NOT NULL,
    set_name   VARCHAR,
    crawled_at TIMESTAMPTZ  DEFAULT now(),
    PRIMARY KEY (tcg, set_code, language)
);

-- ---------------------------------------------------------------------------
-- Shop metadata
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS yuyutei_sets (
    game_code       VARCHAR      NOT NULL,
    set_code        VARCHAR      NOT NULL,
    set_name        VARCHAR,
    last_crawled_at TIMESTAMPTZ,
    listing_count   INTEGER,
    PRIMARY KEY (game_code, set_code)
);

CREATE TABLE IF NOT EXISTS bigweb_cardsets (
    game_id         INTEGER  NOT NULL,
    cardset_id      INTEGER  NOT NULL,
    set_code        VARCHAR,
    set_name        VARCHAR,
    last_crawled_at TIMESTAMPTZ,
    listing_count   INTEGER,
    PRIMARY KEY (game_id, cardset_id)
);
