import type { MetadataRoute } from 'next'
import { SITE_URL, source } from '@/lib/source'

export const dynamic = 'force-static'

export default function sitemap(): MetadataRoute.Sitemap {
  return [
    { url: `${SITE_URL}/`, priority: 1 },
    ...source.getPages().map((page) => ({
      // GitHub Pages serves an exported page without the trailing slash; with it the page is a 404.
      url: `${SITE_URL}${page.url}`,
      priority: 0.7,
    })),
  ]
}
