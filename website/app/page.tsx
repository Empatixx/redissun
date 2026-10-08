import Link from 'next/link'

const objects = [
  { name: 'Bucket', text: 'A single value with TTL, set-if-absent and compare-and-set.', href: '/docs/objects/bucket' },
  { name: 'HashMap', text: 'A distributed map with HashMap-style methods and streaming iteration.', href: '/docs/objects/hash-map' },
  { name: 'Vec', text: 'A shared list with index access, stored in a Redis list.', href: '/docs/objects/vec' },
  { name: 'VecDeque', text: 'A shared queue or stack with both ends, stored in a Redis list.', href: '/docs/objects/vec-deque' },
  { name: 'HashSet', text: 'A shared set with union, intersection and difference.', href: '/docs/objects/hash-set' },
  { name: 'Lock', text: 'A reentrant lock with a watchdog and pub/sub wake-ups, as in Redisson.', href: '/docs/objects/lock' },
]

export default function Home() {
  return (
    <main className="mx-auto flex min-h-screen max-w-3xl flex-col justify-center gap-10 px-4 py-16">
      <header className="flex flex-col gap-4">
        <h1 className="text-5xl font-bold tracking-tight">redissun</h1>
        <p className="text-lg text-fd-muted-foreground">
          Distributed objects on Redis for Rust, closely inspired by Redisson. Async on tokio,
          serde codecs, atomic operations through Lua.
        </p>
        <div className="flex gap-3">
          <Link
            href="/docs/getting-started/quick-start"
            className="rounded-md bg-fd-primary px-4 py-2 text-sm font-medium text-fd-primary-foreground"
          >
            Quick start
          </Link>
          <Link href="/docs" className="rounded-md border px-4 py-2 text-sm font-medium">
            Documentation
          </Link>
        </div>
      </header>
      <section className="grid gap-4 sm:grid-cols-3">
        {objects.map((object) => (
          <Link key={object.name} href={object.href} className="rounded-lg border p-4 hover:bg-fd-accent">
            <h2 className="font-semibold">{object.name}</h2>
            <p className="mt-1 text-sm text-fd-muted-foreground">{object.text}</p>
          </Link>
        ))}
      </section>
    </main>
  )
}
