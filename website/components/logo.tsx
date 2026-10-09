export function Logo({ className = 'size-6' }: { className?: string }) {
  return (
    <svg viewBox="0 0 64 64" className={className} aria-hidden="true" style={{ color: 'var(--color-fd-primary)' }}>
      <mask id="bite" maskUnits="userSpaceOnUse" x="0" y="0" width="64" height="64">
        <rect width="64" height="64" fill="#fff"/>
        <circle cx="41.5" cy="12.5" r="4" fill="#000"/>
        <circle cx="44.5" cy="19" r="2.6" fill="#000"/>
      </mask>
      <g stroke="currentColor" strokeWidth="3" strokeLinecap="round" fill="none">
        <path d="M12 56 5 60M16 60l-5 4M48 60l5 4M52 56l7 4"/>
        <path d="M12 44 8 32M52 44l4-12"/>
      </g>
      <g fill="currentColor">
        <ellipse cx="32" cy="49" rx="23" ry="13.5"/>
        <circle cx="7.5" cy="26" r="6.5"/>
        <circle cx="56.5" cy="26" r="6.5"/>
      </g>
      <path d="M7.5 19.6 4.8 26l2.7.8ZM56.5 19.6 59.2 26l-2.7.8Z" style={{ fill: 'var(--color-fd-background)' }}/>
      <ellipse cx="32" cy="46.5" rx="12" ry="7" fill="#3a1208"/>
      <g mask="url(#bite)" style={{ stroke: 'var(--color-fd-foreground)' }} strokeWidth="5.6" strokeLinecap="round" strokeLinejoin="round" fill="none">
        <path d="M26 46V12h8.5a7 7 0 0 1 0 14H26M34.5 26 41 44"/>
      </g>
      <path d="M20 46.5a12 7 0 0 0 24 0z" fill="currentColor"/>
      <path d="M20 46.5a12 7 0 0 0 24 0" stroke="#3a1208" strokeWidth="1.4" fill="none" strokeLinecap="round"/>
      <path d="M16 38v-5M48 38v-5" stroke="currentColor" strokeWidth="3" strokeLinecap="round"/>
      <g fill="#fff" stroke="currentColor" strokeWidth="2">
        <circle cx="16" cy="29.5" r="4.4"/>
        <circle cx="48" cy="29.5" r="4.4"/>
      </g>
      <circle cx="17.7" cy="29" r="1.9" fill="#1a1210"/>
      <circle cx="46.3" cy="29" r="1.9" fill="#1a1210"/>
      <g style={{ fill: 'var(--color-fd-foreground)' }} opacity="0.65"><circle cx="48" cy="9" r="1"/><circle cx="50.5" cy="13.5" r="0.8"/><circle cx="47" cy="15" r="0.7"/></g>
    </svg>
  )
}
