import { NextRequest, NextResponse } from 'next/server'
import { query } from '@/lib/db'

interface CardRow {
  card_edition_id: string
  set_code: string
  set_name: string
  card_number: string
  card_name: string
  rarity_code: string
  rarity_name: string
  image_url: string | null
  shop: string | null
  price: number | null
  quantity: number | null
  url: string | null
  condition: string | null
  crawled_at: string | null
}

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ tcg: string }> }
) {
  const { tcg } = await params
  const { searchParams } = new URL(request.url)
  const setCode = searchParams.get('set_code')

  if (!setCode) {
    return NextResponse.json({ error: '缺少 set_code 参数' }, { status: 400 })
  }

  try {
    // Get all cards in the set, joined with the latest shop prices per listing
    const sql = `
      SELECT
        e.card_edition_id,
        e.set_code,
        e.set_name,
        e.card_number,
        e.card_name,
        e.rarity_code,
        e.rarity_name,
        e.image_url,
        p.shop,
        p.price,
        p.quantity,
        p.url,
        p.condition,
        p.crawled_at::text AS crawled_at
      FROM (
        SELECT DISTINCT ON (tcg, card_number, rarity_code)
          md5(tcg || card_number || COALESCE(rarity_code, '')) AS card_edition_id,
          tcg,
          set_code,
          COALESCE(set_name, '') AS set_name,
          card_number,
          card_name,
          COALESCE(rarity_code, '') AS rarity_code,
          COALESCE(rarity_name, '') AS rarity_name,
          (extra::json->>'image_url')::text AS image_url
        FROM raw_official_cards
        WHERE tcg = $1 AND set_code = $2
        ORDER BY tcg, card_number, rarity_code, crawled_at DESC
      ) e
      LEFT JOIN (
        SELECT DISTINCT ON (tcg, card_number_raw, shop, COALESCE(condition, ''))
          tcg,
          card_number_raw,
          shop,
          price,
          quantity,
          url,
          condition,
          crawled_at
        FROM raw_shop_listings
        WHERE tcg = $1
        ORDER BY tcg, card_number_raw, shop, COALESCE(condition, ''), crawled_at DESC
      ) p ON p.tcg = e.tcg AND p.card_number_raw = e.card_number
      ORDER BY e.card_number, e.rarity_code, p.shop NULLS LAST
    `

    const rows = await query<CardRow>(sql, [tcg, setCode])

    // Group rows by card_edition_id
    type CardEntry = {
      card_edition_id: string
      set_code: string
      set_name: string
      card_number: string
      card_name: string
      rarity_code: string
      rarity_name: string
      image_url: string | null
      prices: Array<{
        shop: string
        price: number | null
        quantity: number | null
        url: string | null
        condition: string | null
        crawled_at: string | null
      }>
    }

    const cardMap = new Map<string, CardEntry>()

    for (const row of rows) {
      if (!cardMap.has(row.card_edition_id)) {
        cardMap.set(row.card_edition_id, {
          card_edition_id: row.card_edition_id,
          set_code: row.set_code,
          set_name: row.set_name,
          card_number: row.card_number,
          card_name: row.card_name,
          rarity_code: row.rarity_code,
          rarity_name: row.rarity_name,
          image_url: row.image_url,
          prices: [],
        })
      }
      if (row.shop) {
        cardMap.get(row.card_edition_id)!.prices.push({
          shop: row.shop,
          price: row.price,
          quantity: row.quantity,
          url: row.url,
          condition: row.condition,
          crawled_at: row.crawled_at,
        })
      }
    }

    // Determine the last updated timestamp for this set's price data
    const lastUpdatedResult = await query<{ last_updated: string | null }>(
      `SELECT MAX(rsl.crawled_at)::text AS last_updated
       FROM raw_shop_listings rsl
       WHERE rsl.tcg = $1
         AND rsl.card_number_raw IN (
           SELECT DISTINCT card_number
           FROM raw_official_cards
           WHERE tcg = $1 AND set_code = $2
         )`,
      [tcg, setCode]
    )

    return NextResponse.json({
      cards: Array.from(cardMap.values()),
      last_updated: lastUpdatedResult[0]?.last_updated ?? null,
    })
  } catch (error) {
    console.error('[API] Failed to fetch cards:', error)
    return NextResponse.json(
      { error: '获取卡牌数据失败', detail: String(error) },
      { status: 500 }
    )
  }
}
