'use client'

import { TCGSet } from '@/lib/types'
import { ChevronDown, Layers } from 'lucide-react'
import { useState, useRef, useEffect } from 'react'

interface SetSelectorProps {
  sets: TCGSet[]
  selectedSet: TCGSet | null
  onSelect: (set: TCGSet) => void
  loading?: boolean
}

export function SetSelector({ sets, selectedSet, onSelect, loading }: SetSelectorProps) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    setOpen(false)
  }, [sets])

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  if (loading) {
    return (
      <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-card border border-border text-sm min-w-56">
        <div className="h-3 w-3 rounded-full border-2 border-accent border-t-transparent animate-spin" />
        <span className="text-muted-foreground">加载系列中...</span>
      </div>
    )
  }

  if (sets.length === 0) return null

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen(o => !o)}
        className="flex items-center gap-2 px-3 py-2 rounded-lg bg-card border border-border hover:border-accent/50 transition-all duration-150 text-sm font-medium min-w-56"
        aria-haspopup="listbox"
        aria-expanded={open}
      >
        <Layers size={13} className="text-muted-foreground shrink-0" />
        <span className="flex-1 text-left truncate">
          {selectedSet
            ? `${selectedSet.setCode} ${selectedSet.setName}`
            : '选择系列'}
        </span>
        <ChevronDown
          size={14}
          className={`text-muted-foreground transition-transform duration-150 shrink-0 ${open ? 'rotate-180' : ''}`}
        />
      </button>

      {open && (
        <div
          className="absolute top-full mt-1 left-0 z-50 w-80 max-h-72 overflow-y-auto rounded-lg border border-border bg-card shadow-xl shadow-black/30"
          role="listbox"
          aria-label="卡牌系列"
        >
          {sets.map(set => (
            <button
              key={set.setCode}
              role="option"
              aria-selected={selectedSet?.setCode === set.setCode}
              onClick={() => {
                onSelect(set)
                setOpen(false)
              }}
              className={`
                w-full flex items-center justify-between px-3 py-2.5 text-left transition-colors duration-100
                hover:bg-muted border-b border-border/50 last:border-0
                ${selectedSet?.setCode === set.setCode ? 'bg-accent/10 text-accent' : 'text-foreground'}
              `}
            >
              <div className="flex flex-col min-w-0">
                <div className="flex items-center gap-2">
                  <span className="text-xs font-mono font-bold text-muted-foreground shrink-0">{set.setCode}</span>
                  <span className="text-sm font-medium truncate">{set.setName}</span>
                </div>
              </div>
              <span className="text-xs text-muted-foreground font-mono shrink-0 ml-2">{set.cardCount}张</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
