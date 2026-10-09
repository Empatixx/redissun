import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared'
import { Logo } from '@/components/logo'
import { GithubStars } from '@/components/github-stars'

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <span className="inline-flex items-center gap-2 font-semibold">
          <Logo className="size-7" />
          <span className="text-fd-foreground">redissun</span>
        </span>
      ),
      url: '/',
    },
    links: [
      { text: 'Documentation', url: '/docs', active: 'nested-url' },
      { type: 'custom', children: <GithubStars />, secondary: true },
    ],
  }
}

export function docsOptions(): BaseLayoutProps {
  const { nav } = baseOptions()
  return { nav, links: [] }
}
