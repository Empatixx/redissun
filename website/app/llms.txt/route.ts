import { absoluteLinks, docsLlms } from '@/lib/source'

export const dynamic = 'force-static'

export async function GET() {
  return new Response(absoluteLinks(await docsLlms.index()), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  })
}
