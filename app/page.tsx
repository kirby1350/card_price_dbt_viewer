'use client'

import { TCGGame, TCGSet, CardEdition } from '@/lib/types'
import { GameSelector } from '@/components/game-selector'
import { SetSelector } from '@/components/set-selector'
import { CardGrid } from '@/components/card-grid'
import { Clock, RefreshCw, Database, Layers } from 'lucide-react'
import { useState, useCallback } from 'react'
import useSWR from 'swr'

const fetcher = (url: string) => fetch(url).then(r => r.json())

function formatDate(iso: string): string {
  const d = new Date(iso)
  const now = new Date()
  const diffMs = now.getTime() - d.getTime()
  const diffH = Math.floor(diffMs / 3600000)
  const diffD = Math.floor(diffH / 24)
  if (diffH < 1) return '刚刚更新'
  if (diffH < 24) return `${diffH} 小时前`
  if (diffD < 7) return `${diffD} 天前`
  return d.toLocaleDateString('zh-CN', { year: 'numeric', month: 'short', day: 'numeric' })
}

export default function PriceViewerPage() {
  const [selectedGame, setSelectedGame] = useState<TCGGame | null>(null)
  const [selectedSet, setSelectedSet] = useState<TCGSet | null>(null)

  // 获取游戏列表
  const { data: gamesData } = useSWR<{ games: TCGGame[] }>('/api/games', fetcher)

  // 获取系列列表（依赖所选游戏）
  const { data: setsData, isLoading: setsLoading } = useSWR<{
    sets: TCGSet[]
    lastUpdated: string
  }>(
    selectedGame ? `/api/sets?tcg=${selectedGame.id}` : null,
    fetcher
  )

  // 获取卡牌列表（依赖所选系列）
  const { data: cardsData, isLoading: cardsLoading, mutate: mutateCards } = useSWR<{
    cards: CardEdition[]
    setInfo: { setCode: string; setName: string; totalCards: number; lastUpdated: string }
  }>(
    selectedSet && selectedGame
      ? `/api/cards?tcg=${selectedGame.id}&setCode=${selectedSet.setCode}`
      : null,
    fetcher
  )

  const handleGameSelect = useCallback((game: TCGGame) => {
    setSelectedGame(game)
    setSelectedSet(null)
  }, [])

  const handleSetSelect = useCallback((set: TCGSet) => {
    setSelectedSet(set)
  }, [])

  const games = gamesData?.games ?? []
  const sets = setsData?.sets ?? []
  const cards = cardsData?.cards ?? []
  const setInfo = cardsData?.setInfo
  const lastUpdated = setInfo?.lastUpdated

  return (
    <div className="min-h-screen bg-background">
      {/* 顶部导航栏 */}
      <header className="sticky top-0 z-40 border-b border-border bg-background/95 backdrop-blur-sm">
        <div className="max-w-screen-2xl mx-auto px-4 h-14 flex items-center justify-between gap-4">
          {/* 左侧：Logo + 选择器 */}
          <div className="flex items-center gap-3 min-w-0">
            {/* Logo */}
            <div className="flex items-center gap-2 shrink-0">
              <div className="w-7 h-7 rounded-md bg-accent/20 border border-accent/30 flex items-center justify-center">
                <Database size={14} className="text-accent" />
              </div>
              <span className="text-sm font-bold text-foreground hidden sm:block">TCG Price</span>
            </div>

            {/* 分隔线 */}
            <div className="w-px h-5 bg-border hidden sm:block" />

            {/* 游戏选择器 */}
            {games.length > 0 && (
              <GameSelector
                games={games}
                selectedGame={selectedGame}
                onSelect={handleGameSelect}
              />
            )}

            {/* 箭头 */}
            {selectedGame && sets.length > 0 && (
              <>
                <span className="text-muted-foreground/40 text-sm hidden sm:block">/</span>
                <SetSelector
                  sets={sets}
                  selectedSet={selectedSet}
                  onSelect={handleSetSelect}
                  loading={setsLoading}
                />
              </>
            )}
          </div>

          {/* 右侧：最后更新时间 + 刷新 */}
          <div className="flex items-center gap-3 shrink-0">
            {lastUpdated && (
              <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                <Clock size={12} />
                <span className="hidden sm:block">最后更新：</span>
                <span className="font-mono">{formatDate(lastUpdated)}</span>
              </div>
            )}
            {selectedSet && (
              <button
                onClick={() => mutateCards()}
                className="p-1.5 rounded-md hover:bg-muted transition-colors text-muted-foreground hover:text-foreground"
                title="刷新数据"
              >
                <RefreshCw size={13} />
              </button>
            )}
          </div>
        </div>
      </header>

      {/* 主内容区 */}
      <main className="max-w-screen-2xl mx-auto px-4 py-6">
        {/* 未选择游戏 — 欢迎屏 */}
        {!selectedGame && (
          <div className="flex flex-col items-center justify-center py-24 gap-6">
            <div className="w-16 h-16 rounded-2xl bg-accent/10 border border-accent/20 flex items-center justify-center">
              <Database size={28} className="text-accent" />
            </div>
            <div className="text-center">
              <h1 className="text-2xl font-bold text-foreground text-balance">TCG 卡牌价格查看器</h1>
              <p className="text-sm text-muted-foreground mt-2 text-pretty">
                从左上角选择卡牌游戏，然后选择系列，即可查看各商店最新挂牌价格
              </p>
            </div>
            {/* 游戏快速选择卡片 */}
            {games.length > 0 && (
              <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 mt-4 w-full max-w-lg">
                {games.map(game => (
                  <button
                    key={game.id}
                    onClick={() => handleGameSelect(game)}
                    className="flex flex-col items-start gap-1 p-3 rounded-xl bg-card border border-border hover:border-accent/50 hover:bg-card/80 transition-all duration-150 text-left"
                  >
                    <span className="text-sm font-semibold text-foreground truncate w-full">{game.name}</span>
                    <span className="text-[11px] text-muted-foreground font-mono">{game.nameJa}</span>
                  </button>
                ))}
              </div>
            )}
          </div>
        )}

        {/* 已选游戏但未选系列 */}
        {selectedGame && !selectedSet && !setsLoading && sets.length > 0 && (
          <div className="flex flex-col items-center justify-center py-20 gap-4">
            <div className="w-12 h-12 rounded-xl bg-muted flex items-center justify-center">
              <Layers size={22} className="text-muted-foreground" />
            </div>
            <p className="text-sm text-muted-foreground">请从上方选择一个系列以查看卡牌价格</p>
            {/* 系列快速选择 */}
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-2 mt-2 w-full max-w-2xl">
              {sets.map(set => (
                <button
                  key={set.setCode}
                  onClick={() => handleSetSelect(set)}
                  className="flex flex-col gap-0.5 p-2.5 rounded-lg bg-card border border-border hover:border-accent/50 transition-all duration-150 text-left"
                >
                  <span className="text-xs font-mono font-bold text-accent">{set.setCode}</span>
                  <span className="text-xs text-foreground truncate">{set.setName}</span>
                  <span className="text-[10px] text-muted-foreground font-mono">{set.cardCount} 张</span>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* 系列信息 + 卡牌网格 */}
        {selectedSet && (
          <div className="flex flex-col gap-5">
            {/* 系列标题栏 */}
            {setInfo && (
              <div className="flex flex-wrap items-center justify-between gap-3 pb-4 border-b border-border">
                <div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-mono font-bold text-accent bg-accent/10 px-2 py-0.5 rounded border border-accent/20">
                      {setInfo.setCode}
                    </span>
                    <h2 className="text-lg font-bold text-foreground">{setInfo.setName}</h2>
                  </div>
                  <p className="text-xs text-muted-foreground mt-1 font-mono">
                    共 {setInfo.totalCards} 张卡牌 · 显示前 {cards.length} 张
                  </p>
                </div>
                <div className="flex items-center gap-1.5 text-xs text-muted-foreground bg-muted/50 px-3 py-1.5 rounded-lg border border-border">
                  <Clock size={11} />
                  <span>最后更新：{formatDate(setInfo.lastUpdated)}</span>
                </div>
              </div>
            )}

            {/* 卡牌网格 */}
            <CardGrid cards={cards} loading={cardsLoading} />
          </div>
        )}
      </main>
    </div>
  )
}
