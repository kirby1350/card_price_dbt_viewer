'use client'

import Image from 'next/image'
import { useState } from 'react'
import { ExternalLink } from 'lucide-react'
import { cn, formatPrice, getRarityColor, SHOP_LABELS } from '@/lib/utils'
import type { CardWithPrices } from '@/lib/types'

interface CardPriceItemProps {
  card: CardWithPrices
}

export function CardPriceItem({ card }: CardPriceItemProps) {
  const [imgError, setImgError] = useState(false)
  const imgSrc = !imgError && card.image_url ? card.image_url : '/card-placeholder.svg'

  const shopPrices = card.prices.reduce<
    Record<string, { price: number | null; quantity: number | null; url: string | null }>
  >((acc, p) => {
    const existing = acc[p.shop]
    if (!existing || (p.price !== null && (existing.price === null || p.price < existing.price))) {
      acc[p.shop] = { price: p.price, quantity: p.quantity, url: p.url }
    }
    return acc
  }, {})

  const shops = Object.keys(shopPrices)

  return (
    <article className="flex flex-col bg-card border border-border rounded-lg overflow-hidden hover:border-accent/60 transition-colors group">
      <div className="relative bg-muted aspect-[5/7] overflow-hidden">
        <Image
          src={imgSrc}
          alt={card.card_name}
          fill
          sizes="(max-width: 640px) 50vw, (max-width: 1024px) 33vw, 20vw"
          className="object-contain p-1 group-hover:scale-[1.04] transition-transform duration-300"
          crossOrigin="anonymous"
          onError={() => setImgError(true)}
          unoptimized={!!card.image_url}
        />
        {card.rarity_code && (
          <span
            className={cn(
              'absolute top-1.5 right-1.5 text-xs font-bold font-mono bg-background/80 backdrop-blur-sm px-1.5 py-0.5 rounded',
              getRarityColor(card.rarity_code)
            )}
          >
            {card.rarity_code}
          </span>
        )}
      </div>

      <div className="flex flex-col gap-2 p-2.5 flex-1">
        <div>
          <p className="text-xs font-mono text-muted-foreground leading-tight">{card.card_number}</p>
          <p className="text-sm font-medium leading-snug line-clamp-2 text-foreground mt-0.5">
            {card.card_name}
          </p>
        </div>
        <div className="border-t border-border" />
        {shops.length > 0 ? (
          <div className="flex flex-col gap-1.5 mt-auto">
            {shops.map((shop) => {
              const { price, quantity, url } = shopPrices[shop]
              const label = SHOP_LABELS[shop] ?? shop
              return (
                <div key={shop} className="flex items-center justify-between gap-1">
                  <div className="flex items-center gap-1 min-w-0">
                    <span className="text-xs text-muted-foreground truncate">{label}</span>
                    {url && (
                      <a
                        href={url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-muted-foreground hover:text-accent transition-colors shrink-0"
                        aria-label={`在 ${label} 查看 ${card.card_name}`}
                      >
                        <ExternalLink size={10} />
                      </a>
                    )}
                  </div>
                  <div className="flex items-center gap-2 shrink-0">
                    {quantity !== null && (
                      <span className="text-xs text-muted-foreground font-mono">×{quantity}</span>
                    )}
                    <span
                      className={cn(
                        'text-sm font-semibold font-mono tabular-nums',
                        price === null
                          ? 'text-muted-foreground'
                          : price >= 10000
                            ? 'text-price-high'
                            : price >= 1000
                              ? 'text-price-mid'
                              : 'text-price-low'
                      )}
                    >
                      {formatPrice(price)}
                    </span>
                  </div>
                </div>
              )
            })}
          </div>
        ) : (
          <p className="text-xs text-muted-foreground mt-auto">暂无价格数据</p>
        )}
      </div>
    </article>
  )
}
