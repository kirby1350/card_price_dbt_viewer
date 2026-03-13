import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatPrice(price: number | null, currency = 'JPY'): string {
  if (price === null || price === undefined) return '—'
  return new Intl.NumberFormat('ja-JP', {
    style: 'currency',
    currency,
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(price)
}

export function formatDate(dateStr: string | null): string {
  if (!dateStr) return '—'
  const date = new Date(dateStr)
  return new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    timeZone: 'Asia/Tokyo',
  }).format(date)
}

export const SHOP_LABELS: Record<string, string> = {
  yuyutei: 'YuYuTei',
  bigweb: 'Bigweb',
  torecatchi: 'トレカッチ',
  mercari: 'メルカリ',
  rakuten: '楽天',
}

export const RARITY_COLORS: Record<string, string> = {
  N:   'text-muted-foreground',
  C:   'text-muted-foreground',
  R:   'text-blue-400',
  SR:  'text-yellow-400',
  RR:  'text-yellow-300',
  RRR: 'text-orange-400',
  SP:  'text-pink-400',
  SEC: 'text-purple-400',
  UR:  'text-cyan-300',
  SCR: 'text-cyan-200',
  RH:  'text-blue-300',
  SRH: 'text-yellow-300',
  LR:  'text-red-400',
  DEFAULT: 'text-foreground',
}

export function getRarityColor(rarity: string): string {
  return RARITY_COLORS[rarity] ?? RARITY_COLORS.DEFAULT
}
