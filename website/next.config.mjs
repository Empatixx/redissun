import { createMDX } from 'fumadocs-mdx/next'

const withMDX = createMDX()

const config = {
  output: 'export',
  basePath: '/redgrid',
  images: { unoptimized: true },
}

export default withMDX(config)
