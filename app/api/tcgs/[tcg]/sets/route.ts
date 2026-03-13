import { NextRequest, NextResponse } from 'next/server'
import { query } from '@/lib/db'

interface SetRow {
  set_code: string
  set_name: string
  total_cards: number | null
  last_updated: string | null
}

// Map from TCG to its specific sets table
const SET_TABLE_MAP: Record<string, string> = {
  zx: 'zx_sets',
  yugioh: 'yugioh_sets',
  vanguard: 'vanguard_sets',
  weiss: 'weiss_sets',
  digimon: 'digimon_sets',
  unionarena: 'ua_sets',
}

// Some tables have different column names
const SET_QUERY_MAP: Record<string, string> = {
  zx: `SELECT set_code, set_name, total_cards, crawled_at AS last_updated FROM zx_sets ORDER BY set_code`,
  yugioh: `SELECT set_code, set_name, total_cards, crawled_at AS last_updated FROM yugioh_sets WHERE set_code IS NOT NULL ORDER BY release_date DESC NULLS LAST, set_code`,
  vanguard: `SELECT set_code, set_name, total_cards, crawled_at AS last_updated FROM vanguard_sets WHERE set_code IS NOT NULL ORDER BY set_code`,
  weiss: `SELECT set_code, set_name, total_cards, crawled_at AS last_updated FROM weiss_sets WHERE set_code IS NOT NULL ORDER BY set_code`,
  digimon: `SELECT set_code, set_name, total_cards, crawled_at AS last_updated FROM digimon_sets WHERE set_code IS NOT NULL ORDER BY set_code`,
  unionarena: `SELECT set_code, set_name, total_cards, crawled_at AS last_updated FROM ua_sets WHERE set_code IS NOT NULL ORDER BY set_code`,
}

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ tcg: string }> }
) {
  const { tcg } = await params

  if (!SET_TABLE_MAP[tcg]) {
    return NextResponse.json({ error: '不支持的TCG类型' }, { status: 400 })
  }

  try {
    const sql = SET_QUERY_MAP[tcg]
    const rows = await query<SetRow>(sql)
    return NextResponse.json(rows)
  } catch (error) {
    console.error('[API] Failed to fetch sets:', error)
    return NextResponse.json(
      { error: '获取系列数据失败', detail: String(error) },
      { status: 500 }
    )
  }
}
