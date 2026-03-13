'use client'

import { cn } from '@/lib/utils'
import type { TCGSet } from '@/lib/types'

interface SetSelectorProps {
  sets: TCGSet[]
  selected: string | null
  onSelect: (setCode: string) => void
  isLoading?: boolean
}

export function SetSelector({ sets, selected, onSelect, isLoading }: SetSelectorProps) {
  if (isLoading) {
    return (
      <div className="flex gap-2 overflow-x-auto pb-1">
        {Array.from({ length: 10 }).map((_, i) => (
          <div key={i} className="shrink-0 h-9 w-28 rounded bg-muted animate-pulse" />
        ))}
      </div>
    )
  }

  if (sets.length === 0) {
    return <p className="text-muted-foreground text-sm py-2">暂无系列数据</p>
  }

  return (
    <div className="flex gap-2 overflow-x-auto pb-1">
      {sets.map((set) => (
        <button
          key={set.set_code}
          onClick={() => onSelect(set.set_code)}
          title={set.set_name ? `${set.set_code} — ${set.set_name}` : set.set_code}
          className={cn(
            'shrink-0 flex flex-col items-start px-3 py-1.5 rounded text-left transition-colors border',
            selected === set.set_code
              ? 'bg-accent text-accent-foreground border-accent'
              : 'bg-muted text-muted-foreground border-border hover:border-accent hover:text-foreground'
          )}
        >
          <span className="font-mono font-semibold text-xs leading-tight">{set.set_code}</span>
          {set.set_name && (
            <span className="text-xs opacity-70 max-w-[120px] truncate leading-tight mt-0.5">
              {set.set_name}
            </span>
          )}
        </button>
      ))}
    </div>
  )
}
