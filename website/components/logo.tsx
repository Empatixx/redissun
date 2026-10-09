export function Logo({ className = 'size-6' }: { className?: string }) {
  return (
    <svg viewBox="0 0 32 32" className={className} aria-hidden="true">
      <g stroke="currentColor" strokeWidth="2" strokeLinecap="round">
        <path d="M16 2.5v3.5M16 26v3.5M2.5 16H6M26 16h3.5M6.4 6.4l2.5 2.5M23.1 23.1l2.5 2.5M25.6 6.4l-2.5 2.5M8.9 23.1l-2.5 2.5" />
      </g>
      <circle cx="16" cy="16" r="7.5" fill="currentColor" />
      <path d="M13 19.5v-7h3.2a2.1 2.1 0 0 1 0 4.2H13m3.2 0 2 2.8" stroke="var(--color-fd-background)" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" fill="none" />
    </svg>
  )
}
