'use client'

import useSWR from 'swr'
import { useState } from 'react'
import { Clock, Database } from 'lucide-react'
import { TCGSelector } from '@/components/tcg-selector'
import { SetSelector } from '@/components/set-selector'
import { CardGrid } from '@/components/card-grid'
import { formatDate } from '@/lib/utils'
import type { TCG, TCGSet, CardsResponse } from '@/lib/types'

const fetcher = (url: string) => fetch(url).then((r) => r.json())

export default function HomePage() {
  const [selectedTCG, setSelectedTCG] = useState<string | null>(null)
  const [selectedSet, setSelectedSet] = useState<string | null>(null)

  // Fetch TCG list (static, never changes)
  const { data: tcgs = [] } = useSWR<TCG[]>('/api/tcgs', fetcher)

  // Fetch sets for selected TCG
  const { data: sets = [], isLoading: setsLoading } = useSWR<TCGSet[]>(
    selectedTCG ? `/api/tcgs/${selectedTCG}/sets` : null,
    fetcher
  )

  // Fetch cards for selected set
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

  function handleSetSelect(setCode: string) {
    setSelectedSet(setCode)
  }

  const selectedSetInfo = sets.find((s) => s.set_code === selectedSet)

  return (
    <div className="min-h-screen flex flex-col bg-background">
      {/* Header */}
      <header className="sticky top-0 z-40 bg-background/95 backdrop-blur border-b border-border">
        <div className="max-w-screen-2xl mx-auto px-4 py-3 flex flex-col gap-3">
          {/* Top row: logo + TCG selector */}
          <div className="flex items-center gap-4 flex-wrap">
            <div className="flex items-center gap-2 shrink-0">
              <Database size={20} className="text-accent" />
              <h1 className="text-base font-bold text-foreground tracking-tight">
                TCG Price Viewer
              </h1>
            </div>
            <div className="flex-1 min-w-0">
              <TCGSelector
                tcgs={tcgs}
                selected={selectedTCG}
                onSelect={handleTCGSelect}
              />
            </div>
          </div>

          {/* Second row: set selector (shown after TCG is selected) */}
          {selectedTCG && (
            <div className="flex flex-col gap-2">
              <SetSelector
                sets={sets}
                selected={selectedSet}
                onSelect={handleSetSelect}
                isLoading={setsLoading}
              />
            </div>
          )}
        </div>
      </header>

      {/* Main content */}
      <main className="flex-1 max-w-screen-2xl mx-auto w-full px-4 py-4">
        {!selectedTCG ? (
          <div className="flex flex-col items-center justify-center py-32 text-muted-foreground">
            <Database size={48} className="mb-4 opacity-30" />
            <p className="text-xl font-medium">请选择卡牌游戏</p>
            <p className="text-sm mt-1">在左上角选择游戏种类以开始浏览</p>
          </div>
        ) : !selectedSet ? (
          <div className="flex flex-col items-center justify-center py-32 text-muted-foreground">
            <p className="text-xl font-medium">请选择系列</p>
            <p className="text-sm mt-1">
              {setsLoading ? '正在加载系列列表…' : `共 ${sets.length} 个系列`}
            </p>
          </div>
        ) : (
          <div className="flex flex-col gap-4">
            {/* Set info bar */}
            <div className="flex items-center justify-between gap-4 flex-wrap">
              <div>
                <h2 className="text-lg font-bold text-foreground">
                  <span className="font-mono text-accent mr-2">{selectedSet}</span>
                  {selectedSetInfo?.set_name && (
                    <span>{selectedSetInfo.set_name}</span>
                  )}
                </h2>
                {cardsData && (
                  <p className="text-sm text-muted-foreground mt-0.5">
                    共 {cardsData.cards.length} 张卡牌
                  </p>
                )}
              </div>
              {cardsData?.last_updated && (
                <div className="flex items-center gap-1.5 text-xs text-muted-foreground bg-muted px-3 py-1.5 rounded">
                  <Clock size={12} />
                  <span>最后更新：{formatDate(cardsData.last_updated)}</span>
                </div>
              )}
            </div>

            {/* Card grid */}
            <CardGrid
              cards={cardsData?.cards ?? []}
              isLoading={cardsLoading}
            />
          </div>
        )}
      </main>
    </div>
  )
}
