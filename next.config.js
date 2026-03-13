/** @type {import('next').NextConfig} */
const nextConfig = {
  images: {
    remotePatterns: [
      {
        protocol: 'https',
        hostname: 'www.zxtcg.com',
      },
      {
        protocol: 'https',
        hostname: 'www.db.yugioh-card.com',
      },
      {
        protocol: 'https',
        hostname: 'cf-vanguard.com',
      },
      {
        protocol: 'https',
        hostname: 'ws-tcg.com',
      },
      {
        protocol: 'https',
        hostname: 'digimoncard.com',
      },
      {
        protocol: 'https',
        hostname: 'www.unionarena-tcg.com',
      },
      {
        protocol: 'https',
        hostname: '**',
      },
    ],
  },
}

module.exports = nextConfig
