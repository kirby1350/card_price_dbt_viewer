'use client'

import { CardEdition, ShopPrice } from '@/lib/types'
import { ExternalLink, Package } from 'lucide-react'
import { useState } from 'react'

interface CardPriceCardProps {
  card: CardEdition
}

const RARITY_COLORS: Record<string, string> = {
  SEC: 'text-yellow-300 border-yellow-400/40 bg-yellow-400/10',
  LSR: 'text-amber-300 border-amber-400/40 bg-amber-400/10',
  LR: 'text-amber-200 border-amber-300/40 bg-amber-300/10',
  SLR: 'text-orange-300 border-orange-400/40 bg-orange-400/10',
  SR: 'text-purple-300 border-purple-400/40 bg-purple-400/10',
  R: 'text-blue-300 border-blue-400/40 bg-blue-400/10',
  UC: 'text-cyan-300 border-cyan-400/40 bg-cyan-400/10',
  C: 'text-muted-foreground border-border bg-muted/30',
  UR: 'text-yellow-300 border-yellow-400/40 bg-yellow-400/10',
  GR: 'text-amber-300 border-amber-400/40 bg-amber-400/10',
  CR: 'text-orange-300 border-orange-400/40 bg-orange-400/10',
  SCR: 'text-yellow-200 border-yellow-300/40 bg-yellow-300/10',
  RRR: 'text-purple-300 border-purple-400/40 bg-purple-400/10',
  RR: 'text-blue-300 border-blue-400/40 bg-blue-400/10',
  PR: 'text-pink-300 border-pink-400/40 bg-pink-400/10',
}

function getRarityStyle(rarityCode: string): string {
  return RARITY_COLORS[rarityCode] ?? 'text-muted-foreground border-border bg-muted/30'
}

function formatPrice(price: number | null): string {
  if (price === null) return '―'
  return `¥${price.toLocaleString()}`
}

interface ShopPriceRowProps {
  shopPrice: ShopPrice
}

function ShopPriceRow({ shopPrice }: ShopPriceRowProps) {
  const hasPrice = shopPrice.price !== null && shopPrice.price > 0
  const hasStock = shopPrice.quantity !== null && shopPrice.quantity > 0

  return (
    <div className="flex items-center justify-between py-1.5 border-b border-border/50 last:border-0">
      <div className="flex items-center gap-1.5 min-w-0">
        <span className="text-xs font-medium text-muted-foreground shrink-0 w-16">
          {shopPrice.shopLabel}
        </span>
        {hasPrice && shopPrice.url ? (
          <a
            href={shopPrice.url}
            target="_blank"
            rel="noopener noreferrer"
            className="text-xs font-mono font-bold text-accent hover:text-accent/80 transition-colors flex items-center gap-0.5"
          >
            {formatPrice(shopPrice.price)}
            <ExternalLink size={9} className="shrink-0" />
          </a>
        ) : (
          <span className={`text-xs font-mono font-bold ${hasPrice ? 'text-foreground' : 'text-muted-foreground/60'}`}>
            {formatPrice(shopPrice.price)}
          </span>
        )}
      </div>

      <div className="flex items-center gap-1 shrink-0">
        <Package size={10} className={hasStock ? 'text-success' : 'text-muted-foreground/40'} />
        <span className={`text-xs font-mono ${hasStock ? 'text-success' : 'text-muted-foreground/40'}`}>
          {shopPrice.quantity ?? 0}
        </span>
      </div>
    </div>
  )
}

export function CardPriceCard({ card }: CardPriceCardProps) {
  const [imgError, setImgError] = useState(false)
  const rarityStyle = getRarityStyle(card.rarityCode)

  const hasAnyPrice = card.shopPrices.some(sp => sp.price !== null && sp.price > 0)
  const minPrice = card.shopPrices
    .map(sp => sp.price)
    .filter((p): p is number => p !== null && p > 0)
    .reduce((min, p) => (p < min ? p : min), Infinity)

  return (
    <article
      className={`
        relative flex flex-col rounded-lg border bg-card overflow-hidden
        transition-all duration-200 hover:border-accent/40 hover:shadow-lg hover:shadow-accent/5
        ${!hasAnyPrice ? 'opacity-60' : ''}
      `}
    >
      {/* 稀有度徽章 */}
      <div className="absolute top-2 left-2 z-10">
        <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold font-mono border ${rarityStyle}`}>
          {card.rarityCode}
        </span>
      </div>

      {/* 最低价格徽章 */}
      {hasAnyPrice && (
        <div className="absolute top-2 right-2 z-10">
          <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold font-mono bg-background/80 text-accent border border-accent/30 backdrop-blur-sm">
            {formatPrice(minPrice === Infinity ? null : minPrice)}
          </span>
        </div>
      )}

      {/* 卡牌图片区域 */}
      <div className="relative w-full bg-muted/30 overflow-hidden" style={{ aspectRatio: '2/3' }}>
        {card.imageUrl && !imgError ? (
          <img
            src={card.imageUrl}
            alt={card.cardName}
            className="w-full h-full object-cover"
            onError={() => setImgError(true)}
            crossOrigin="anonymous"
            loading="lazy"
          />
        ) : (
          <div className="w-full h-full flex flex-col items-center justify-center gap-2 p-3">
            <div className="w-12 h-12 rounded-lg bg-muted flex items-center justify-center">
              <span className={`text-lg font-bold font-mono ${rarityStyle.split(' ')[0]}`}>
                {card.rarityCode[0]}
              </span>
            </div>
            <span className="text-[10px] text-muted-foreground text-center leading-tight">
              {card.cardNumber}
            </span>
          </div>
        )}
      </div>

      {/* 卡牌信息与价格 */}
      <div className="flex flex-col flex-1 p-2.5 gap-2">
        {/* 卡号和名称 */}
        <div>
          <div className="text-[10px] font-mono text-muted-foreground">{card.cardNumber}</div>
          <div className="text-xs font-medium text-foreground leading-tight mt-0.5 line-clamp-2" title={card.cardName}>
            {card.cardName}
          </div>
        </div>

        {/* 商店价格列表 */}
        <div className="flex flex-col">
          {card.shopPrices.map(shopPrice => (
            <ShopPriceRow key={shopPrice.shop} shopPrice={shopPrice} />
          ))}
        </div>
      </div>
    </article>
  )
}
