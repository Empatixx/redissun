export const BASE_PATH = '/redissun'

export function Logo({ className = 'size-6' }: { className?: string }) {
  return (
    // eslint-disable-next-line @next/next/no-img-element
    <img
      src={`${BASE_PATH}/logo.png`}
      alt="redissun: a crab eating the letter R"
      className={`${className} object-contain`}
    />
  )
}
