import { docs } from 'collections/server'
import { llms, loader } from 'fumadocs-core/source'

export const SITE_URL = 'https://empatixx.github.io/redissun'

export const source = loader({
  baseUrl: '/docs',
  source: docs.toFumadocsSource(),
})

export const docsLlms = llms(source, {
  renderPage: async (page) => `# ${page.data.title} (${SITE_URL}${page.url})

${await page.data.getText('processed')}`,
})

export function absoluteLinks(text: string): string {
  return text.replaceAll('](/docs', `](${SITE_URL}/docs`)
}

export function markdownUrl(slugs: string[]): string {
  return `/llms.mdx/docs/${[...slugs, 'content.md'].join('/')}`
}
