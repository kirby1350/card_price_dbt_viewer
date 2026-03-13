'use client'

import { TCGGame } from '@/lib/types'
import { ChevronDown } from 'lucide-react'
import { useState, useRef, useEffect } from 'react'

interface GameSelectorProps {
  games: TCGGame[]
  selectedGame: TCGGame | null
  onSelect: (game: TCGGame) => void
}

export function GameSelector({ games, selectedGame, onSelect }: GameSelectorProps) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen(o => !o)}
        className="flex items-center gap-2 px-3 py-2 rounded-lg bg-card border border-border hover:border-accent/50 transition-all duration-150 text-sm font-medium min-w-48"
        aria-haspopup="listbox"
        aria-expanded={open}
      >
        <span className="flex-1 text-left truncate">
          {selectedGame ? selectedGame.name : '选择卡牌游戏'}
        </span>
        <ChevronDown
          size={14}
          className={`text-muted-foreground transition-transform duration-150 shrink-0 ${open ? 'rotate-180' : ''}`}
        />
      </button>

      {open && (
        <div
          className="absolute top-full mt-1 left-0 z-50 w-72 rounded-lg border border-border bg-card shadow-xl shadow-black/30 overflow-hidden"
          role="listbox"
          aria-label="卡牌游戏"
        >
          {games.map(game => (
            <button
              key={game.id}
              role="option"
              aria-selected={selectedGame?.id === game.id}
              onClick={() => {
                onSelect(game)
                setOpen(false)
              }}
              className={`
                w-full flex flex-col items-start px-3 py-2.5 text-left transition-colors duration-100
                hover:bg-muted border-b border-border/50 last:border-0
                ${selectedGame?.id === game.id ? 'bg-accent/10 text-accent' : 'text-foreground'}
              `}
            >
              <span className="text-sm font-medium">{game.name}</span>
              <span className="text-xs text-muted-foreground font-mono">{game.nameJa}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
