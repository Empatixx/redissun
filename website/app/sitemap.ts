import type { MetadataRoute } from 'next'
import { SITE_URL, source } from '@/lib/source'

export const dynamic = 'force-static'

export default function sitemap(): MetadataRoute.Sitemap {
  return [
    { url: `${SITE_URL}/`, priority: 1 },
    ...source.getPages().map((page) => ({
      url: `${SITE_URL}${page.url}/`,
      priority: 0.7,
    })),
  ]
}
