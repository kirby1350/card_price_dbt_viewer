'use client'

import { CardPriceItem } from './card-price-item'
import type { CardWithPrices } from '@/lib/types'

interface CardGridProps {
  cards: CardWithPrices[]
  isLoading?: boolean
}

export function CardGrid({ cards, isLoading }: CardGridProps) {
  if (isLoading) {
    return (
      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-3">
        {Array.from({ length: 24 }).map((_, i) => (
          <div key={i} className="bg-card border border-border rounded-lg overflow-hidden">
            <div className="aspect-[5/7] bg-muted animate-pulse" />
            <div className="p-2.5 flex flex-col gap-2">
              <div className="h-3 w-14 bg-muted rounded animate-pulse" />
              <div className="h-4 w-full bg-muted rounded animate-pulse" />
              <div className="border-t border-border mt-1" />
              <div className="h-3 w-3/4 bg-muted rounded animate-pulse" />
              <div className="h-3 w-1/2 bg-muted rounded animate-pulse" />
            </div>
          </div>
        ))}
      </div>
    )
  }

  if (cards.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-32 text-muted-foreground">
        <p className="text-lg font-medium">暂无卡牌数据</p>
        <p className="text-sm mt-1 opacity-70">此系列尚未爬取数据</p>
      </div>
    )
  }

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-3">
      {cards.map((card) => (
        <CardPriceItem key={card.card_edition_id} card={card} />
      ))}
    </div>
  )
}
