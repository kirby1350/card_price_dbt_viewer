'use client'

import useSWR from 'swr'
import { useState } from 'react'
import { Clock, Layers } from 'lucide-react'
import { TCGSelector } from '@/components/tcg-selector'
import { SetSelector } from '@/components/set-selector'
import { CardGrid } from '@/components/card-grid'
import { formatDate } from '@/lib/utils'
import type { TCG, TCGSet, CardsResponse } from '@/lib/types'

const fetcher = (url: string) => fetch(url).then((r) => r.json())

export default function HomePage() {
  const [selectedTCG, setSelectedTCG] = useState<string | null>(null)
  const [selectedSet, setSelectedSet] = useState<string | null>(null)

  const { data: tcgs = [] } = useSWR<TCG[]>('/api/tcgs', fetcher)

  const { data: sets = [], isLoading: setsLoading } = useSWR<TCGSet[]>(
    selectedTCG ? `/api/tcgs/${selectedTCG}/sets` : null,
    fetcher
  )

  const { data: cardsData, isLoading: cardsLoading } = useSWR<CardsResponse>(
    selectedTCG && selectedSet
      ? `/api/tcgs/${selectedTCG}/cards?set_code=${encodeURIComponent(selectedSet)}`
      : null,
    fetcher
  )

  function handleTCGSelect(tcg: string) {
    setSelectedTCG(tcg)
    setSelectedSet(null)
  }

  const selectedSetInfo = sets.find((s) => s.set_code === selectedSet)

  return (
    <div className="min-h-screen flex flex-col bg-background">
      <header className="sticky top-0 z-40 bg-background/95 backdrop-blur border-b border-border">
        <div className="max-w-screen-2xl mx-auto px-4 py-3 flex flex-col gap-3">
          <div className="flex items-center gap-4 flex-wrap">
            <div className="flex items-center gap-2 shrink-0">
              <Layers size={18} className="text-accent" />
              <h1 className="text-sm font-bold text-foreground tracking-tight whitespace-nowrap">
                TCG Price Viewer
              </h1>
            </div>
            <div className="flex-1 min-w-0">
              <TCGSelector tcgs={tcgs} selected={selectedTCG} onSelect={handleTCGSelect} />
            </div>
          </div>
          {selectedTCG && (
            <SetSelector
              sets={sets}
              selected={selectedSet}
              onSelect={setSelectedSet}
              isLoading={setsLoading}
            />
          )}
        </div>
      </header>

      <main className="flex-1 max-w-screen-2xl mx-auto w-full px-4 py-5">
        {!selectedTCG ? (
          <div className="flex flex-col items-center justify-center py-40 text-muted-foreground">
            <Layers size={48} className="mb-4 opacity-20" />
            <p className="text-xl font-semibold text-foreground">请选择卡牌游戏</p>
            <p className="text-sm mt-2">在上方选择游戏种类以开始浏览价格</p>
          </div>
        ) : !selectedSet ? (
          <div className="flex flex-col items-center justify-center py-40 text-muted-foreground">
            <p className="text-xl font-semibold text-foreground">请选择系列</p>
            <p className="text-sm mt-2">
              {setsLoading ? '正在加载系列列表…' : `共 ${sets.length} 个系列可选`}
            </p>
          </div>
        ) : (
          <div className="flex flex-col gap-4">
            <div className="flex items-start justify-between gap-4 flex-wrap">
              <div>
                <h2 className="text-lg font-bold text-foreground flex items-center gap-2 flex-wrap">
                  <span className="font-mono text-accent">{selectedSet}</span>
                  {selectedSetInfo?.set_name && (
                    <span className="font-sans font-semibold">{selectedSetInfo.set_name}</span>
                  )}
                </h2>
                <p className="text-sm text-muted-foreground mt-0.5">
                  {cardsLoading
                    ? '正在加载卡牌数据…'
                    : cardsData
                      ? `共 ${cardsData.cards.length} 张卡牌`
                      : ''}
                </p>
              </div>
              {cardsData?.last_updated && (
                <div className="flex items-center gap-1.5 text-xs text-muted-foreground bg-muted border border-border px-3 py-1.5 rounded">
                  <Clock size={12} />
                  <span>价格更新：{formatDate(cardsData.last_updated)}</span>
                </div>
              )}
            </div>
            <CardGrid cards={cardsData?.cards ?? []} isLoading={cardsLoading} />
          </div>
        )}
      </main>
    </div>
  )
}
