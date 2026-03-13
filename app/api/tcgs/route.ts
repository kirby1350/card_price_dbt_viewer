import { NextResponse } from 'next/server'

// Static TCG catalog matching dbt/seeds/tcg_catalog.csv
const TCG_CATALOG = [
  { tcg: 'zx',         tcg_name: 'Z/X -Zillions of enemy X-', region: 'japan' },
  { tcg: 'yugioh',     tcg_name: 'Yu-Gi-Oh!',                 region: 'global' },
  { tcg: 'vanguard',   tcg_name: 'Cardfight!! Vanguard',       region: 'japan' },
  { tcg: 'weiss',      tcg_name: 'Weiss Schwarz',              region: 'japan' },
  { tcg: 'digimon',    tcg_name: 'Digimon Card Game',          region: 'japan' },
  { tcg: 'unionarena', tcg_name: 'Union Arena TCG',            region: 'japan' },
]

export async function GET() {
  return NextResponse.json(TCG_CATALOG)
}
