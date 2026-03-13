'use client'

import { CardEdition } from '@/lib/types'
import { CardPriceCard } from './card-price-card'
import { LayoutGrid, List, Search, SlidersHorizontal } from 'lucide-react'
import { useState, useMemo } from 'react'

interface CardGridProps {
  cards: CardEdition[]
  loading?: boolean
}

const RARITY_ORDER = ['SEC', 'LSR', 'LR', 'SLR', 'SR', 'UR', 'SCR', 'GR', 'CR', 'PR', 'RRR', 'R', 'RR', 'UC', 'U', 'C']

function getRarityOrder(rarity: string): number {
  const idx = RARITY_ORDER.indexOf(rarity)
  return idx === -1 ? 99 : idx
}

export function CardGrid({ cards, loading }: CardGridProps) {
  const [search, setSearch] = useState('')
  const [sortBy, setSortBy] = useState<'number' | 'price' | 'rarity'>('number')
  const [filterRarity, setFilterRarity] = useState<string>('all')

  const rarities = useMemo(() => {
    const set = new Set(cards.map(c => c.rarityCode))
    return ['all', ...RARITY_ORDER.filter(r => set.has(r)), ...[...set].filter(r => !RARITY_ORDER.includes(r))]
  }, [cards])

  const filteredCards = useMemo(() => {
    let result = cards

    if (search.trim()) {
      const q = search.trim().toLowerCase()
      result = result.filter(c =>
        c.cardNumber.toLowerCase().includes(q) ||
        c.cardName.toLowerCase().includes(q)
      )
    }

    if (filterRarity !== 'all') {
      result = result.filter(c => c.rarityCode === filterRarity)
    }

    return [...result].sort((a, b) => {
      if (sortBy === 'number') {
        return a.cardNumber.localeCompare(b.cardNumber)
      }
      if (sortBy === 'rarity') {
        return getRarityOrder(a.rarityCode) - getRarityOrder(b.rarityCode)
      }
      if (sortBy === 'price') {
        const aMin = Math.min(...a.shopPrices.map(sp => sp.price ?? Infinity))
        const bMin = Math.min(...b.shopPrices.map(sp => sp.price ?? Infinity))
        return aMin - bMin
      }
      return 0
    })
  }, [cards, search, sortBy, filterRarity])

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center py-24 gap-4">
        <div className="w-10 h-10 rounded-full border-2 border-accent border-t-transparent animate-spin" />
        <span className="text-sm text-muted-foreground">加载卡牌数据中...</span>
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-4">
      {/* 工具栏 */}
      <div className="flex flex-wrap items-center gap-3">
        {/* 搜索框 */}
        <div className="relative flex-1 min-w-40">
          <Search size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none" />
          <input
            type="text"
            placeholder="搜索卡号或卡名..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="w-full pl-8 pr-3 py-2 rounded-lg bg-card border border-border text-sm placeholder:text-muted-foreground/60 focus:outline-none focus:border-accent/60 transition-colors"
          />
        </div>

        {/* 稀有度筛选 */}
        <div className="flex items-center gap-1.5 flex-wrap">
          <SlidersHorizontal size={13} className="text-muted-foreground" />
          {rarities.map(r => (
            <button
              key={r}
              onClick={() => setFilterRarity(r)}
              className={`px-2 py-1 rounded text-xs font-mono font-medium border transition-all duration-100
                ${filterRarity === r
                  ? 'bg-accent text-accent-foreground border-accent'
                  : 'bg-card text-muted-foreground border-border hover:border-accent/40 hover:text-foreground'
                }`}
            >
              {r === 'all' ? '全部' : r}
            </button>
          ))}
        </div>

        {/* 排序 */}
        <select
          value={sortBy}
          onChange={e => setSortBy(e.target.value as 'number' | 'price' | 'rarity')}
          className="px-2.5 py-2 rounded-lg bg-card border border-border text-sm text-foreground focus:outline-none focus:border-accent/60 transition-colors cursor-pointer"
        >
          <option value="number">按卡号</option>
          <option value="rarity">按稀有度</option>
          <option value="price">按价格</option>
        </select>

        {/* 结果数量 */}
        <span className="text-xs text-muted-foreground font-mono">
          {filteredCards.length} / {cards.length} 张
        </span>
      </div>

      {/* 卡牌网格 */}
      {filteredCards.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-16 gap-3">
          <LayoutGrid size={32} className="text-muted-foreground/30" />
          <span className="text-sm text-muted-foreground">未找到匹配的卡牌</span>
        </div>
      ) : (
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 2xl:grid-cols-7 gap-3">
          {filteredCards.map(card => (
            <CardPriceCard key={card.cardEditionId} card={card} />
          ))}
        </div>
      )}
    </div>
  )
}
