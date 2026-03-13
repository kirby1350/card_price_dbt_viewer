import { NextRequest, NextResponse } from 'next/server'
import { MOCK_SETS } from '@/lib/mock-data'

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url)
  const tcg = searchParams.get('tcg')

  if (!tcg) {
    return NextResponse.json({ error: '缺少 tcg 参数' }, { status: 400 })
  }

  const sets = MOCK_SETS[tcg] ?? []
  const lastUpdated = sets.length > 0
    ? sets.reduce((latest, s) => s.lastUpdated > latest ? s.lastUpdated : latest, sets[0].lastUpdated)
    : new Date().toISOString()

  return NextResponse.json({ sets, lastUpdated })
}
