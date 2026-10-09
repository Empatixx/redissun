export function Logo({ className = 'size-6' }: { className?: string }) {
  return (
    <svg viewBox="0 0 64 64" className={className} aria-hidden="true" style={{ color: 'var(--color-fd-primary)' }}>
      <mask id="bite" maskUnits="userSpaceOnUse" x="0" y="0" width="64" height="64">
        <rect width="64" height="64" fill="#fff"/>
        <circle cx="52.5" cy="10" r="4" fill="#000"/>
        <circle cx="55.5" cy="16.5" r="2.6" fill="#000"/>
      </mask>
      <g stroke="currentColor" strokeWidth="3" strokeLinecap="round" fill="none">
        <path d="M10 52 3 57M12 57l-6 6M20 59l-3 4M35 59l3 4M43 56l5 4"/>
        <path d="M14 43 9 38M45 45 49 44"/>
      </g>
      <g fill="currentColor">
        <ellipse cx="27" cy="49" rx="19" ry="12.5"/>
        <circle cx="8.5" cy="34" r="6.5"/>
        <circle cx="51.5" cy="42" r="6.8"/>
      </g>
      <path d="M8.5 27.6 5.8 34l2.7.8Z" style={{ fill: 'var(--color-fd-background)' }}/>
      <g mask="url(#bite)" style={{ stroke: 'var(--color-fd-foreground)' }} strokeWidth="5.6" strokeLinecap="round" strokeLinejoin="round" fill="none">
        <path d="M38 8h8a6.4 6.4 0 0 1 0 12.8H38M46 20.8 52 40"/>
        <path d="M38 8v29"/>
      </g>
      <path d="M51.5 42 49 47l-2.4-1.6" fill="none" style={{ stroke: 'var(--color-fd-background)' }} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
      <path d="M21 41v-4M33 41v-4" stroke="currentColor" strokeWidth="3" strokeLinecap="round"/>
      <g fill="#fff" stroke="currentColor" strokeWidth="2">
        <circle cx="21" cy="34.5" r="4.2"/>
        <circle cx="33" cy="34.5" r="4.2"/>
      </g>
      <circle cx="22.7" cy="33.8" r="1.8" fill="#1a1210"/>
      <circle cx="34.7" cy="33.8" r="1.8" fill="#1a1210"/>
      <path d="M23 50q7 9 15 0z" fill="#3a1208" stroke="#3a1208" strokeWidth="1.4" strokeLinejoin="round"/>
      <g style={{ fill: 'var(--color-fd-foreground)' }} opacity="0.65"><circle cx="58" cy="9" r="1"/><circle cx="59.5" cy="13.5" r="0.8"/><circle cx="57.5" cy="19" r="0.8"/></g>
    </svg>
  )
}
