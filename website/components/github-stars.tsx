import { fetchRepositoryInfo } from 'fumadocs-ui/components/github-info'
import { Star } from 'lucide-react'
import { GithubMark } from './github-mark'

export const REPO_OWNER = 'Empatixx'
export const REPO_NAME = 'redissun'

async function stars(): Promise<number | null> {
  try {
    const token = process.env.GITHUB_TOKEN
    const info = await fetchRepositoryInfo({
      owner: REPO_OWNER,
      repo: REPO_NAME,
      token,
    })
    return info.stars
  } catch {
    return null
  }
}

export async function GithubStars({ className = '' }: { className?: string }) {
  const count = await stars()
  return (
    <a
      href={`https://github.com/${REPO_OWNER}/${REPO_NAME}`}
      target="_blank"
      rel="noreferrer noopener"
      className={`inline-flex items-center gap-2 rounded-md border px-3 py-1.5 text-sm font-medium text-fd-foreground/80 transition-colors hover:bg-fd-accent hover:text-fd-accent-foreground ${className}`}
    >
      <GithubMark className="size-4" />
      <span>GitHub</span>
      {count !== null && (
        <span className="inline-flex items-center gap-1 border-s ps-2 text-fd-muted-foreground">
          <Star className="size-3.5" />
          {new Intl.NumberFormat('en', { notation: 'compact' }).format(count)}
        </span>
      )}
    </a>
  )
}
