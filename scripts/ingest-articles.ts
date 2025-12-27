import 'dotenv/config';
import fs from 'node:fs/promises';
import path from 'node:path';
import matter from 'gray-matter';
import { createClient } from '@supabase/supabase-js';
import { lookup as mimeLookup } from 'mime-types';

/* eslint-disable
  @typescript-eslint/no-unsafe-assignment,
  @typescript-eslint/no-unsafe-call,
  @typescript-eslint/no-unsafe-member-access,
  @typescript-eslint/no-unsafe-argument
*/

type Database = {
  public: {
    Tables: {
      articles: {
        Row: {
          id: string;
          slug: string;
          title: string;
          excerpt: string | null;
          content_mdx: string;
          cover_image_url: string | null;
          published_at: string | null;
          created_at: string;
          updated_at: string;
        };
        Insert: {
          slug: string;
          title: string;
          excerpt?: string | null;
          content_mdx: string;
          cover_image_url?: string | null;
          published_at?: string | null;
        };
        Update: Partial<Database['public']['Tables']['articles']['Insert']>;
        Relationships: [];
      };
    };
    Views: Record<string, never>;
    Functions: Record<string, never>;
    Enums: Record<string, never>;
    CompositeTypes: Record<string, never>;
  };
};

type Frontmatter = {
  slug?: string;
  title?: string;
  date?: string; // YYYY-MM-DD
  excerpt?: string;
  coverImage?: string; // path/URL
};

const CONTENT_DIR =
  process.env.ARTICLES_DIR ?? path.resolve(__dirname, '../content/articles');

const ASSETS_DIR =
  process.env.ARTICLE_ASSETS_DIR ??
  path.resolve(__dirname, '../content/assets');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_API_KEY = process.env.SUPABASE_API_KEY;
const BUCKET = process.env.SUPABASE_ARTICLES_BUCKET ?? 'articles';
const DRY_RUN = process.env.DRY_RUN === '1';

if (!SUPABASE_URL) throw new Error('Missing SUPABASE_URL');
if (!SUPABASE_API_KEY) throw new Error('Missing SUPABASE_API_KEY');

const supabase = createClient<Database>(SUPABASE_URL, SUPABASE_API_KEY, {
  auth: { persistSession: false },
});

function uniq<T>(arr: T[]) {
  return Array.from(new Set(arr));
}

function findLocalAssetRefs(mdx: string): string[] {
  const refs: string[] = [];

  // Markdown images: ![alt](path)
  for (const m of mdx.matchAll(
    /!\[[^\]]*]\(([^)\s]+)(?:\s+["'][^"']*["'])?\)/g,
  )) {
    refs.push(m[1]);
  }

  // JSX/HTML attributes: src="..." or src='...'
  for (const m of mdx.matchAll(/\bsrc\s*=\s*["']([^"']+)["']/g)) {
    refs.push(m[1]);
  }

  return uniq(refs).filter(
    (r) => !/^https?:\/\//i.test(r) && !/^data:/i.test(r),
  );
}

async function fileExists(p: string) {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

function resolveLocalFilePath(ref: string, mdxFileDir: string) {
  if (ref.startsWith('/')) {
    // Treat as article-local asset: /x.jpg -> hart-property-api/content/assets/x.jpg
    return path.join(ASSETS_DIR, ref.slice(1));
  }

  // Relative file next to the MDX
  return path.resolve(mdxFileDir, ref);
}

async function uploadToSupabase({
  slug,
  localFilePath,
  originalRef,
}: {
  slug: string;
  localFilePath: string;
  originalRef: string;
}) {
  const filename = path.basename(localFilePath);
  const objectKey = `${slug}/${filename}`;
  const contentType = mimeLookup(filename) || 'application/octet-stream';
  const file = await fs.readFile(localFilePath);
  const bucket = supabase.storage.from(BUCKET);

  if (DRY_RUN) {
    const { data } = bucket.getPublicUrl(objectKey);
    const publicUrl = data.publicUrl;
    return { objectKey, publicUrl, originalRef };
  }

  const { error } = await bucket.upload(objectKey, file, {
    upsert: true,
    contentType: String(contentType),
  });
  if (error) throw new Error(error.message);

  const { data } = bucket.getPublicUrl(objectKey);
  return { objectKey, publicUrl: data.publicUrl, originalRef };
}

async function ingestOne(filePath: string) {
  const raw = await fs.readFile(filePath, 'utf8');
  const parsed = matter(raw);
  const fm = (parsed.data ?? {}) as Frontmatter;
  const mdxFileDir = path.dirname(filePath);

  const slug = fm.slug ?? path.basename(filePath).replace(/\.mdx?$/i, '');

  const title =
    fm.title ?? parsed.content.match(/^#\s+(.+)$/m)?.[1]?.trim() ?? slug;

  const excerpt =
    fm.excerpt ??
    parsed.content
      .split('\n')
      .find((l) => l.trim() && !l.startsWith('#'))
      ?.trim() ??
    null;

  const published_at = fm.date ?? null;

  let content = parsed.content;

  const refs = findLocalAssetRefs(content);
  const replacementMap = new Map<string, string>();

  for (const ref of refs) {
    const localPath = resolveLocalFilePath(ref, mdxFileDir);
    const ok = await fileExists(localPath);
    if (!ok) {
      // Skip refs we can't resolve (e.g. /bullet.svg used by CSS, etc.)
      continue;
    }

    const { publicUrl } = await uploadToSupabase({
      slug,
      localFilePath: localPath,
      originalRef: ref,
    });
    replacementMap.set(ref, publicUrl);
  }

  for (const [ref, url] of replacementMap.entries()) {
    content = content.split(ref).join(url);
  }

  let cover_image_url: string | null = null;
  if (fm.coverImage) {
    cover_image_url = replacementMap.get(fm.coverImage) ?? fm.coverImage;
  } else {
    // If we uploaded anything, use the first uploaded asset as a reasonable default cover.
    const firstUploaded = replacementMap.values().next().value as
      | string
      | undefined;
    cover_image_url = firstUploaded ?? null;
  }

  const row = {
    slug,
    title,
    excerpt,
    content_mdx: content,
    cover_image_url,
    published_at,
  };

  if (DRY_RUN) {
    console.log(`[DRY_RUN] upsert article: ${slug}`, {
      ...row,
      content_mdx: `${row.content_mdx.slice(0, 180)}...`,
    });
    return;
  }

  const { error } = await supabase
    .from('articles')
    .upsert(row, { onConflict: 'slug' });

  if (error) throw new Error(error.message);
  console.log(`Upserted article: ${slug}`);
}

async function main() {
  const files = (await fs.readdir(CONTENT_DIR))
    .filter((f) => f.endsWith('.mdx') || f.endsWith('.md'))
    .map((f) => path.join(CONTENT_DIR, f));

  if (!files.length) {
    throw new Error(`No .mdx/.md files found in ${CONTENT_DIR}`);
  }

  for (const file of files) {
    await ingestOne(file);
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
