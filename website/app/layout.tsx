import { RootProvider } from 'fumadocs-ui/provider/next'
import { Inter } from 'next/font/google'
import type { ReactNode } from 'react'
import type { Metadata } from 'next'
import './global.css'

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  title: {
    template: '%s | redissun',
    default: "redissun — Rust's std collections and locks, shared through Redis",
  },
  description:
    "Rust's std collections and locks, shared through Redis. Inspired by Redisson: maps, queues, locks, caches and streams on tokio and serde.",
  openGraph: {
    images: ['https://empatixx.github.io/redissun/logo.png'],
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
