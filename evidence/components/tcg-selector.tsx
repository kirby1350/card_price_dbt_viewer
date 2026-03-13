'use client'

import { cn } from '@/lib/utils'
import type { TCG } from '@/lib/types'

interface TCGSelectorProps {
  tcgs: TCG[]
  selected: string | null
  onSelect: (tcg: string) => void
}

export function TCGSelector({ tcgs, selected, onSelect }: TCGSelectorProps) {
  return (
    <div className="flex items-center gap-2 flex-wrap">
      <span className="text-muted-foreground text-sm font-medium shrink-0">游戏：</span>
      <div className="flex gap-2 flex-wrap">
        {tcgs.map((tcg) => (
          <button
            key={tcg.tcg}
            onClick={() => onSelect(tcg.tcg)}
            className={cn(
              'px-3 py-1.5 rounded text-sm font-medium transition-colors border',
              selected === tcg.tcg
                ? 'bg-accent text-accent-foreground border-accent'
                : 'bg-muted text-muted-foreground border-border hover:border-accent hover:text-foreground'
            )}
          >
            {tcg.tcg_name}
          </button>
        ))}
      </div>
    </div>
  )
}
