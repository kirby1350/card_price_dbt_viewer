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
      <div className="flex gap-2 overflow-x-auto pb-2">
        {Array.from({ length: 8 }).map((_, i) => (
          <div
            key={i}
            className="shrink-0 h-9 w-24 rounded bg-muted animate-pulse"
          />
        ))}
      </div>
    )
  }

  if (sets.length === 0) {
    return (
      <p className="text-muted-foreground text-sm">暂无系列数据</p>
    )
  }

  return (
    <div className="flex gap-2 overflow-x-auto pb-2 scrollbar-thin">
      {sets.map((set) => (
        <button
          key={set.set_code}
          onClick={() => onSelect(set.set_code)}
          title={set.set_name ?? set.set_code}
          className={cn(
            'shrink-0 px-3 py-1.5 rounded text-sm transition-colors border whitespace-nowrap',
            selected === set.set_code
              ? 'bg-accent text-accent-foreground border-accent'
              : 'bg-muted text-muted-foreground border-border hover:border-accent hover:text-foreground'
          )}
        >
          <span className="font-mono font-semibold">{set.set_code}</span>
          {set.set_name && (
            <span className="ml-1.5 text-xs opacity-70 max-w-[140px] truncate inline-block align-middle">
              {set.set_name}
            </span>
          )}
        </button>
      ))}
    </div>
  )
}
