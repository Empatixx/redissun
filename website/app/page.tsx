import Link from 'next/link'
import { HomeLayout } from 'fumadocs-ui/layouts/home'
import { DynamicCodeBlock } from 'fumadocs-ui/components/dynamic-codeblock'
import { baseOptions } from '@/lib/layout.shared'
import { GithubStars, REPO_NAME, REPO_OWNER } from '@/components/github-stars'
import { Logo } from '@/components/logo'

type Entry = { name: string; text: string; slug: string }

const groups: { title: string; entries: Entry[] }[] = [
  {
    title: 'Data',
    entries: [
      { name: 'Bucket', text: 'One value under one key, with TTL and compare-and-set.', slug: 'bucket' },
      { name: 'HashMap', text: 'A shared map with std-style methods and streaming iteration.', slug: 'hash-map' },
      { name: 'Vec', text: 'A shared list with index access.', slug: 'vec' },
      { name: 'VecDeque', text: 'A queue or stack, with blocking pops.', slug: 'vec-deque' },
      { name: 'HashSet', text: 'A shared set with union, intersection and difference.', slug: 'hash-set' },
      { name: 'SortedSet', text: 'Values ordered by a score. A leaderboard or a priority queue.', slug: 'sorted-set' },
      { name: 'Geo', text: 'Members with a position, and searches around a point.', slug: 'geo' },
      { name: 'AtomicI64', text: 'A shared counter.', slug: 'atomic-i64' },
    ],
  },
  {
    title: 'Caches',
    entries: [
      { name: 'HashMapCache', text: 'A map with per-entry TTL, idle time, max size and events.', slug: 'hash-map-cache' },
      { name: 'HashSetCache', text: 'A set whose values expire.', slug: 'hash-set-cache' },
      { name: 'LocalCachedMap', text: 'A map with a cache in your program, kept in sync.', slug: 'local-cached-map' },
    ],
  },
  {
    title: 'Locks and limits',
    entries: [
      { name: 'Lock', text: 'A reentrant lock with a watchdog.', slug: 'lock' },
      { name: 'FairLock', text: 'A lock that serves waiters in order.', slug: 'fair-lock' },
      { name: 'FencedLock', text: 'A lock that gives every owner a higher token.', slug: 'fenced-lock' },
      { name: 'RwLock', text: 'Many readers or one writer.', slug: 'rw-lock' },
      { name: 'MultiLock', text: 'Several locks taken as one.', slug: 'multi-lock' },
      { name: 'Semaphore', text: 'Limit how many programs work at once.', slug: 'semaphore' },
      { name: 'CountDownLatch', text: 'Wait until others have finished.', slug: 'count-down-latch' },
      { name: 'RateLimiter', text: 'A sliding window rate limit.', slug: 'rate-limiter' },
    ],
  },
  {
    title: 'Messaging',
    entries: [
      { name: 'Topic', text: 'Publish and subscribe over Redis pub/sub.', slug: 'topic' },
      { name: 'Stream', text: 'A log with consumer groups.', slug: 'stream' },
      { name: 'DelayedQueue', text: 'Values that arrive in a queue after a delay.', slug: 'delayed-queue' },
    ],
  },
  {
    title: 'Compact and bulk',
    entries: [
      { name: 'BitSet', text: 'A shared array of bits.', slug: 'bit-set' },
      { name: 'HyperLogLog', text: 'Count different values in 12 kB.', slug: 'hyper-log-log' },
      { name: 'BloomFilter', text: 'Remember many values in little memory.', slug: 'bloom-filter' },
      { name: 'Batch', text: 'Send many commands in one round trip.', slug: 'batch' },
    ],
  },
]

const features = [
  {
    title: 'Redisson, in Rust',
    text: 'The same Redis data layout and Lua scripts that Redisson has used in production for years. Rust names where Rust has a type.',
  },
  {
    title: 'Plain async Rust',
    text: 'Built on tokio and serde. Arguments are borrowed, results are owned, and every waiting call takes .timeout().',
  },
  {
    title: 'Atomic by design',
    text: 'Every multi-step operation is one Lua script, and time comes from the Redis server, so clients with different clocks agree.',
  },
]

const example = `use redissun::Client;
use std::time::Duration;

let client = Client::builder().url("redis://127.0.0.1:6379").build().await?;

let users = client.hash_map::<String, String>("users");
users.insert("jirka", "Jirka").await?;

let guard = client.lock("order:42").lock().timeout(Duration::from_secs(5)).await?;
if let Some(guard) = guard {
    guard.unlock().await?;
}`

function Badge({ src, alt, href }: { src: string; alt: string; href: string }) {
  return (
    <a href={href} target="_blank" rel="noreferrer noopener">
      {/* eslint-disable-next-line @next/next/no-img-element */}
      <img src={src} alt={alt} className="h-5" />
    </a>
  )
}

export default function Home() {
  const repo = `https://github.com/${REPO_OWNER}/${REPO_NAME}`
  return (
    <HomeLayout {...baseOptions()}>
      <div className="hero-glow">
        <section className="mx-auto flex max-w-4xl flex-col items-center gap-6 px-4 pb-16 pt-20 text-center">
          <Logo className="size-14 text-fd-primary" />
          <h1 className="text-5xl font-bold tracking-tight sm:text-6xl">redissun</h1>
          <p className="max-w-2xl text-lg text-fd-muted-foreground">
            Distributed objects on Redis for Rust, closely inspired by Redisson: maps, queues,
            locks, caches, streams and more, as ordinary async Rust types.
          </p>
          <div className="flex flex-wrap justify-center gap-2">
            <Badge href="https://crates.io/crates/redissun" alt="crates.io" src="https://img.shields.io/crates/v/redissun?color=c2461f" />
            <Badge href="https://docs.rs/redissun" alt="docs.rs" src="https://img.shields.io/docsrs/redissun?color=c2461f" />
            <Badge href={`${repo}/actions/workflows/ci.yml`} alt="CI" src={`https://img.shields.io/github/actions/workflow/status/${REPO_OWNER}/${REPO_NAME}/ci.yml?branch=main`} />
            <Badge href={`${repo}/blob/main/LICENSE`} alt="License" src="https://img.shields.io/crates/l/redissun?color=c2461f" />
          </div>
          <div className="flex flex-wrap items-center justify-center gap-3">
            <Link
              href="/docs/getting-started/quick-start"
              className="rounded-md bg-fd-primary px-5 py-2.5 text-sm font-medium text-fd-primary-foreground shadow-sm transition-opacity hover:opacity-90"
            >
              Quick start
            </Link>
            <Link href="/docs" className="rounded-md border px-5 py-2.5 text-sm font-medium transition-colors hover:bg-fd-accent">
              Documentation
            </Link>
            <GithubStars className="py-2.5" />
          </div>
          <code className="rounded-md border bg-fd-card px-4 py-2 font-mono text-sm">
            <span className="text-fd-muted-foreground">$ </span>cargo add redissun
          </code>
        </section>
      </div>

      <section className="mx-auto max-w-4xl px-4 pb-16">
        <DynamicCodeBlock lang="rust" code={example} />
      </section>

      <section className="mx-auto grid max-w-5xl gap-4 px-4 pb-16 sm:grid-cols-3">
        {features.map((feature) => (
          <div key={feature.title} className="rounded-lg border bg-fd-card p-5">
            <h2 className="font-semibold text-fd-primary">{feature.title}</h2>
            <p className="mt-2 text-sm text-fd-muted-foreground">{feature.text}</p>
          </div>
        ))}
      </section>

      <section className="mx-auto flex max-w-5xl flex-col gap-10 px-4 pb-20">
        {groups.map((group) => (
          <div key={group.title}>
            <h2 className="mb-3 text-sm font-semibold uppercase tracking-wider text-fd-muted-foreground">
              {group.title}
            </h2>
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              {group.entries.map((entry) => (
                <Link
                  key={entry.name}
                  href={`/docs/objects/${entry.slug}`}
                  className="group rounded-lg border bg-fd-card p-4 transition-colors hover:border-fd-primary/50 hover:bg-fd-accent"
                >
                  <h3 className="font-semibold group-hover:text-fd-primary">{entry.name}</h3>
                  <p className="mt-1 text-sm text-fd-muted-foreground">{entry.text}</p>
                </Link>
              ))}
            </div>
          </div>
        ))}
      </section>

      <footer className="border-t py-8 text-center text-sm text-fd-muted-foreground">
        <p>
          Apache-2.0 · <a className="underline" href={repo}>GitHub</a> ·{' '}
          <a className="underline" href="https://crates.io/crates/redissun">crates.io</a> ·{' '}
          <a className="underline" href="/redissun/llms.txt">llms.txt</a>
        </p>
      </footer>
    </HomeLayout>
  )
}
