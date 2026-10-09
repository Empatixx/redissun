import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared'
import { Logo } from '@/components/logo'
import { GithubStars } from '@/components/github-stars'

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <span className="inline-flex items-center gap-2 font-semibold text-fd-primary">
          <Logo />
          <span className="text-fd-foreground">redissun</span>
        </span>
      ),
      url: '/',
    },
    links: [
      { text: 'Documentation', url: '/docs', active: 'nested-url' },
      { text: 'crates.io', url: 'https://crates.io/crates/redissun', external: true },
      { text: 'docs.rs', url: 'https://docs.rs/redissun', external: true },
      { type: 'custom', children: <GithubStars />, secondary: true },
    ],
  }
}
