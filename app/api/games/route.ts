import { NextResponse } from 'next/server'
import { MOCK_GAMES } from '@/lib/mock-data'

export async function GET() {
  return NextResponse.json({ games: MOCK_GAMES })
}
