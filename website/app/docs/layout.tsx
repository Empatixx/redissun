import { source } from '@/lib/source'
import { docsOptions } from '@/lib/layout.shared'
import { SidebarLinks } from '@/components/sidebar-links'
import { DocsLayout } from 'fumadocs-ui/layouts/docs'
import type { ReactNode } from 'react'

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <DocsLayout
      tree={source.getPageTree()}
      {...docsOptions()}
      sidebar={{ footer: <SidebarLinks /> }}
    >
      {children}
    </DocsLayout>
  )
}
