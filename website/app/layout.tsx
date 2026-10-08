import { RootProvider } from 'fumadocs-ui/provider/next'
import { Inter } from 'next/font/google'
import type { ReactNode } from 'react'
import type { Metadata } from 'next'
import './global.css'

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  title: {
    template: '%s | redgrid',
    default: 'redgrid — distributed objects on Redis for Rust',
  },
  description:
    'Redisson-inspired distributed Map, Bucket and reentrant Lock with watchdog for Rust, on tokio and serde.',
}

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning className={inter.className}>
      <body className="overflow-x-hidden">
        <RootProvider search={{ options: { type: 'static', api: '/redgrid/api/search' } }}>
          {children}
        </RootProvider>
      </body>
    </html>
  )
}
