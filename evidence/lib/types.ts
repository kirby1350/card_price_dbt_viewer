export interface TCG {
  tcg: string
  tcg_name: string
  region: string
}

export interface TCGSet {
  set_code: string
  set_name: string | null
  total_cards: number | null
  last_updated: string | null
}

export interface ShopPrice {
  shop: string
  price: number | null
  quantity: number | null
  url: string | null
  condition: string | null
  crawled_at: string | null
}

export interface CardWithPrices {
  card_edition_id: string
  set_code: string
  set_name: string
  card_number: string
  card_name: string
  rarity_code: string
  rarity_name: string
  image_url: string | null
  prices: ShopPrice[]
}

export interface CardsResponse {
  cards: CardWithPrices[]
  last_updated: string | null
}
