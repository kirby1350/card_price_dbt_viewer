import { TCGGame, TCGSet, CardEdition, ShopPrice } from './types'

// ── TCG 游戏种类 ──────────────────────────────────────────────
export const MOCK_GAMES: TCGGame[] = [
  { id: 'zx', name: 'Z/X -Zillions of enemy X-', nameJa: 'ゼクス', region: 'japan' },
  { id: 'yugioh', name: 'Yu-Gi-Oh!', nameJa: '遊☆戯☆王', region: 'global' },
  { id: 'digimon', name: 'Digimon Card Game', nameJa: 'デジモンカードゲーム', region: 'japan' },
  { id: 'vanguard', name: 'Cardfight!! Vanguard', nameJa: 'カードファイト!! ヴァンガード', region: 'japan' },
  { id: 'weiss', name: 'Weiss Schwarz', nameJa: 'ヴァイスシュヴァルツ', region: 'japan' },
  { id: 'unionarena', name: 'Union Arena TCG', nameJa: 'ユニオンアリーナ', region: 'japan' },
]

// ── Z/X 系列模拟数据 ──────────────────────────────────────────
export const MOCK_SETS: Record<string, TCGSet[]> = {
  zx: [
    { setCode: 'B01', setName: '異世界との邂逅', tcg: 'zx', cardCount: 102, lastUpdated: '2025-06-20T08:30:00Z' },
    { setCode: 'B02', setName: '炎剣の凱旋', tcg: 'zx', cardCount: 110, lastUpdated: '2025-06-20T08:30:00Z' },
    { setCode: 'B03', setName: '黒き銃弾と堕天の翼', tcg: 'zx', cardCount: 117, lastUpdated: '2025-06-18T12:00:00Z' },
    { setCode: 'B04', setName: '緑の守護と地底の王', tcg: 'zx', cardCount: 115, lastUpdated: '2025-06-15T09:00:00Z' },
    { setCode: 'B05', setName: '白き聖域と青の征服者', tcg: 'zx', cardCount: 120, lastUpdated: '2025-06-10T10:00:00Z' },
    { setCode: 'E01', setName: 'Exciting!! ゼクス', tcg: 'zx', cardCount: 50, lastUpdated: '2025-06-08T11:00:00Z' },
    { setCode: 'E02', setName: 'Brilliant!! ゼクス', tcg: 'zx', cardCount: 55, lastUpdated: '2025-06-05T09:30:00Z' },
    { setCode: 'P01', setName: 'プロモーションカード Vol.1', tcg: 'zx', cardCount: 20, lastUpdated: '2025-05-30T14:00:00Z' },
  ],
  yugioh: [
    { setCode: 'PHNI', setName: 'Phantom Nightmare', tcg: 'yugioh', cardCount: 101, lastUpdated: '2025-06-19T10:00:00Z' },
    { setCode: 'LEDE', setName: 'Legacy of Destruction', tcg: 'yugioh', cardCount: 101, lastUpdated: '2025-06-17T08:00:00Z' },
    { setCode: 'AGOV', setName: 'Age of Overlord', tcg: 'yugioh', cardCount: 100, lastUpdated: '2025-06-12T09:00:00Z' },
    { setCode: 'WIHO', setName: 'Wild Survivors', tcg: 'yugioh', cardCount: 76, lastUpdated: '2025-06-09T11:00:00Z' },
  ],
  digimon: [
    { setCode: 'BT01', setName: 'New Evolution', tcg: 'digimon', cardCount: 112, lastUpdated: '2025-06-20T07:00:00Z' },
    { setCode: 'BT02', setName: 'Ultimate Power', tcg: 'digimon', cardCount: 112, lastUpdated: '2025-06-18T08:00:00Z' },
    { setCode: 'BT03', setName: 'Union Impact', tcg: 'digimon', cardCount: 112, lastUpdated: '2025-06-15T10:00:00Z' },
    { setCode: 'EX01', setName: 'Classic Collection', tcg: 'digimon', cardCount: 53, lastUpdated: '2025-06-10T12:00:00Z' },
  ],
  vanguard: [
    { setCode: 'DZ-BT13', setName: '龍剣双覇', tcg: 'vanguard', cardCount: 94, lastUpdated: '2025-06-19T09:00:00Z' },
    { setCode: 'DZ-BT12', setName: '剣竜起動', tcg: 'vanguard', cardCount: 94, lastUpdated: '2025-06-14T11:00:00Z' },
    { setCode: 'DZ-BT11', setName: '覚醒の龍神', tcg: 'vanguard', cardCount: 94, lastUpdated: '2025-06-08T09:00:00Z' },
  ],
  weiss: [
    { setCode: 'W134', setName: 'ぼっち・ざ・ろっく！', tcg: 'weiss', cardCount: 100, lastUpdated: '2025-06-20T06:00:00Z' },
    { setCode: 'W132', setName: '呪術廻戦', tcg: 'weiss', cardCount: 100, lastUpdated: '2025-06-16T08:00:00Z' },
    { setCode: 'W130', setName: 'デデデデ デデデの旗', tcg: 'weiss', cardCount: 100, lastUpdated: '2025-06-12T10:00:00Z' },
  ],
  unionarena: [
    { setCode: 'UA11BT', setName: '銀魂', tcg: 'unionarena', cardCount: 82, lastUpdated: '2025-06-20T05:00:00Z' },
    { setCode: 'UA10BT', setName: 'BLEACH 千年血戦篇', tcg: 'unionarena', cardCount: 82, lastUpdated: '2025-06-15T07:00:00Z' },
    { setCode: 'UA09BT', setName: 'HUNTER×HUNTER', tcg: 'unionarena', cardCount: 82, lastUpdated: '2025-06-10T09:00:00Z' },
  ],
}

// ── 生成模拟卡牌图片URL ──────────────────────────────────────
function getCardImageUrl(tcg: string, setCode: string, cardNumber: string): string | null {
  if (tcg === 'zx') {
    return `https://www.zxtcg.com/assets/card_img/${setCode}/${cardNumber}.png`
  }
  return null
}

// ── 生成模拟价格 ─────────────────────────────────────────────
function generateMockPrice(rarityCode: string, seed: number): { yuyutei: number | null; bigweb: number | null } {
  const rarityMultiplier: Record<string, number> = {
    'SEC': 80, 'LSR': 60, 'LR': 50, 'SLR': 45, 'SR': 25, 'R': 8, 'UC': 3, 'C': 1,
    'Ultra': 30, 'Super': 15, 'Rare': 6, 'Common': 1,
    'SCR': 100, 'UR': 40, 'GR': 35, 'CR': 28, 'PR': 20,
    'RRR': 20, 'RR': 8, 'R': 4, 'U': 2, 'C': 1,
  }
  const base = (rarityMultiplier[rarityCode] ?? 3) * 100
  const jitter = (seed % 50) * 10
  const yuyutei = base + jitter
  const bigweb = Math.random() > 0.3 ? Math.floor(yuyutei * (0.85 + Math.random() * 0.3)) : null
  return { yuyutei, bigweb }
}

// ── 生成模拟卡牌数据 ─────────────────────────────────────────
const ZX_RARITIES = ['SEC', 'LSR', 'LR', 'SR', 'R', 'UC', 'C']
const ZX_CARD_NAMES_B01 = [
  '運命の猟犬ライラプス', '炎槍のシャルロット', '氷壁の守護者ヴァルキリー',
  '黄金の剣士グラディウス', '深淵の魔王バルムンク', '光の天使アリエル',
  '鋼鉄の巨人ゴレム', '風の妖精シルフ', '大地の精霊ノーム', '水の竜神ルーシア',
  '炎の戦士カグツチ', '雷霆の神トール', '月光の女神セレーネ', '太陽の英雄アポロン',
  '闇の騎士モルガン', '星の守護者レグルス', '森の女王アルテミス', '海の王ポセイドン',
  '時の賢者クロノス', '命の源ガイア', '破壊神シヴァ', '創造神ブラフマー',
  '守護神ヴィシュヌ', '戦神マルス', '愛の女神ヴィーナス', '知恵の神アテナ',
  '商業神ヘルメス', '農業神デメテル', '火の神ヘパイストス', '冥界の神ハデス',
]

export function generateMockCards(tcg: string, setCode: string, setName: string): CardEdition[] {
  const cards: CardEdition[] = []
  const rarities = ZX_RARITIES
  const cardNames = ZX_CARD_NAMES_B01

  const count = MOCK_SETS[tcg]?.find(s => s.setCode === setCode)?.cardCount ?? 30
  const displayCount = Math.min(count, 30)

  for (let i = 1; i <= displayCount; i++) {
    const paddedNum = String(i).padStart(3, '0')
    const cardNumber = `${setCode}-${paddedNum}`
    const rarityIndex = Math.floor((i - 1) / (displayCount / rarities.length))
    const rarityCode = rarities[Math.min(rarityIndex, rarities.length - 1)]
    const cardName = cardNames[(i - 1) % cardNames.length]
    const { yuyutei, bigweb } = generateMockPrice(rarityCode, i * 17 + setCode.charCodeAt(0))

    const shopPrices: ShopPrice[] = [
      {
        shop: 'yuyutei',
        shopLabel: 'YuYuTei',
        price: yuyutei,
        quantity: yuyutei ? Math.floor(Math.random() * 8) + 1 : 0,
        url: yuyutei ? `https://yuyu-tei.jp/game_zx/sell/list.php?search%5Bfreeword%5D=${cardNumber}` : null,
        currency: 'JPY',
        condition: 'NM',
      },
      {
        shop: 'bigweb',
        shopLabel: 'Bigweb',
        price: bigweb,
        quantity: bigweb ? Math.floor(Math.random() * 5) + 1 : 0,
        url: bigweb ? `https://www.bigweb.co.jp/ver2/topts.php?TCG_ID=4&product_id=${cardNumber}` : null,
        currency: 'JPY',
        condition: 'NM',
      },
    ]

    const lastUpdated = MOCK_SETS[tcg]?.find(s => s.setCode === setCode)?.lastUpdated ?? new Date().toISOString()

    cards.push({
      cardEditionId: `${tcg}_${cardNumber}_${rarityCode}`,
      tcg,
      setCode,
      setName,
      cardNumber,
      cardName,
      rarityCode,
      rarityName: rarityCode,
      imageUrl: getCardImageUrl(tcg, setCode, cardNumber),
      shopPrices,
      lastUpdated,
    })
  }

  return cards
}
