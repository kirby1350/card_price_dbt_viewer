import { NextRequest, NextResponse } from 'next/server'
import { MOCK_SETS, generateMockCards } from '@/lib/mock-data'

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url)
  const tcg = searchParams.get('tcg')
  const setCode = searchParams.get('setCode')

  if (!tcg || !setCode) {
    return NextResponse.json({ error: '缺少 tcg 或 setCode 参数' }, { status: 400 })
  }

  const setInfo = MOCK_SETS[tcg]?.find(s => s.setCode === setCode)
  if (!setInfo) {
    return NextResponse.json({ error: '未找到该系列' }, { status: 404 })
  }

  const cards = generateMockCards(tcg, setCode, setInfo.setName)

  return NextResponse.json({
    cards,
    setInfo: {
      setCode: setInfo.setCode,
      setName: setInfo.setName,
      totalCards: setInfo.cardCount,
      lastUpdated: setInfo.lastUpdated,
    },
  })
}
