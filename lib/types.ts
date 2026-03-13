// TCG 游戏种类
export interface TCGGame {
  id: string
  name: string
  nameJa: string
  region: string
}

// 系列/套牌
export interface TCGSet {
  setCode: string
  setName: string
  tcg: string
  cardCount: number
  lastUpdated: string
}

// 商店价格信息
export interface ShopPrice {
  shop: string
  shopLabel: string
  price: number | null
  quantity: number | null
  url: string | null
  currency: string
  condition: string
}

// 卡牌版本信息
export interface CardEdition {
  cardEditionId: string
  tcg: string
  setCode: string
  setName: string
  cardNumber: string
  cardName: string
  rarityCode: string
  rarityName: string
  imageUrl: string | null
  shopPrices: ShopPrice[]
  lastUpdated: string
}

// API 响应类型
export interface TCGListResponse {
  games: TCGGame[]
}

export interface SetListResponse {
  sets: TCGSet[]
  lastUpdated: string
}

export interface CardListResponse {
  cards: CardEdition[]
  setInfo: {
    setCode: string
    setName: string
    totalCards: number
    lastUpdated: string
  }
}
