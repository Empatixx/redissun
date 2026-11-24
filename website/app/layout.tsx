import { RootProvider } from 'fumadocs-ui/provider/next'
import { Inter } from 'next/font/google'
import type { ReactNode } from 'react'
import type { Metadata } from 'next'
import './global.css'
import { SITE_URL } from '@/lib/source'

const inter = Inter({ subsets: ['latin'] })

const description =
  'Redisson for Rust: a Redis library with std-style collections, distributed locks, caches and streams. Maps, queues, locks, semaphores and rate limiters on tokio and serde, for Redis, Sentinel and Redis Cluster.'

export const metadata: Metadata = {
  metadataBase: new URL(`${SITE_URL}/`),
  title: {
    template: '%s | redissun',
    default: 'redissun: Redisson for Rust, a Redis library with distributed locks and collections',
  },
  description,
  keywords: [
    'Redisson for Rust',
    'Rust Redis library',
    'Rust Redis client',
    'Redis distributed lock Rust',
    'Redisson',
    'Redis',
    'Valkey',
    'tokio',
  ],
  alternates: { canonical: `${SITE_URL}/` },
  openGraph: {
    type: 'website',
    siteName: 'redissun',
    url: `${SITE_URL}/`,
    title: 'redissun: Redisson for Rust',
    description,
    images: [`${SITE_URL}/logo.png`],
  },
  twitter: {
    card: 'summary',
    title: 'redissun: Redisson for Rust',
    description,
    images: [`${SITE_URL}/logo.png`],
  },
}

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning className={inter.className}>
      <body className="overflow-x-hidden">
        <RootProvider search={{ options: { type: 'static', api: '/redissun/api/search' } }}>
          {children}
        </RootProvider>
      </body>
    </html>
  )
}
