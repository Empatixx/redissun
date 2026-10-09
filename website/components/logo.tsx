export function Logo({ className = 'size-6' }: { className?: string }) {
  return (
    <svg viewBox="0 0 64 64" className={className} aria-hidden="true">
      <g stroke="currentColor" strokeWidth="2.4" strokeLinecap="round" opacity="0.35">
        <path d="M32 3v5M12 9l3.5 3.5M52 9l-3.5 3.5M5 24h5M54 24h5" />
      </g>
      <circle cx="32" cy="24" r="12" fill="currentColor" opacity="0.18" />
      <g stroke="currentColor" strokeWidth="3" strokeLinecap="round" fill="none">
        <path d="M17 38 9 42M17 44l-9 5M20 50l-7 8M47 38l8 4M47 44l9 5M44 50l7 8" />
        <path d="M20 34 12 24M44 34l8-10" />
      </g>
      <g fill="currentColor">
        <ellipse cx="32" cy="42" rx="18" ry="12" />
        <circle cx="11" cy="20" r="7" />
        <circle cx="53" cy="20" r="7" />
      </g>
      <path d="M11 13.5 8.5 20 11 20.6ZM53 13.5 55.5 20 53 20.6Z" fill="var(--color-fd-background)" />
      <g stroke="currentColor" strokeWidth="3" strokeLinecap="round">
        <path d="M25 31v-6M39 31v-6" />
      </g>
      <g fill="#fff" stroke="currentColor" strokeWidth="2">
        <circle cx="25" cy="22.5" r="4" />
        <circle cx="39" cy="22.5" r="4" />
      </g>
      <circle cx="25.6" cy="22.8" r="1.6" fill="#1a1210" />
      <circle cx="38.4" cy="22.8" r="1.6" fill="#1a1210" />
      <path d="M26 41q6 5.5 12 0" stroke="#fff" strokeWidth="2.2" strokeLinecap="round" fill="none" />
    </svg>
  )
}
