import 'dotenv/config';
import fs from 'node:fs/promises';
import path from 'node:path';
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
      listings: {
        Row: {
          id: string;
          external_id: string;
          title: string;
          address: string | null;
          price: string | null;
          psf: string | null;
          size: string | null;
          property_type: string | null;
          status: string;
          distance: string | null;
          link: string | null;
          listed_at: string | null;
          photos: unknown;
          created_at: string;
          updated_at: string;
        };
        Insert: {
          external_id: string;
          title: string;
          address?: string | null;
          price?: string | null;
          psf?: string | null;
          size?: string | null;
          property_type?: string | null;
          status?: string;
          distance?: string | null;
          link?: string | null;
          listed_at?: string | null;
          photos?: unknown;
        };
        Update: Partial<Database['public']['Tables']['listings']['Insert']>;
        Relationships: [];
      };
    };
    Views: Record<string, never>;
    Functions: Record<string, never>;
    Enums: Record<string, never>;
    CompositeTypes: Record<string, never>;
  };
};

type ListingSource = {
  external_id: string;
  title: string;
  address?: string;
  price?: string;
  psf?: string;
  size?: string;
  property_type?: string;
  status?: string;
  distance?: string;
  link?: string;
  listed_at?: string; // YYYY-MM-DD
  photos: string[]; // filenames in assets dir
};

const CONTENT_FILE =
  process.env.LISTINGS_FILE ?? path.resolve(__dirname, '../content/listings.json');
const ASSETS_DIR =
  process.env.LISTINGS_ASSETS_DIR ??
  path.resolve(__dirname, '../content/listings-assets');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_API_KEY = process.env.SUPABASE_API_KEY;
const BUCKET = process.env.SUPABASE_LISTINGS_BUCKET ?? 'listings';
const DRY_RUN = process.env.DRY_RUN === '1';

if (!SUPABASE_URL) throw new Error('Missing SUPABASE_URL');
if (!SUPABASE_API_KEY) throw new Error('Missing SUPABASE_API_KEY');

const supabase = createClient<Database>(SUPABASE_URL, SUPABASE_API_KEY, {
  auth: { persistSession: false },
});

async function uploadPhoto(externalId: string, filename: string) {
  const localPath = path.join(ASSETS_DIR, filename);
  const file = await fs.readFile(localPath);
  const contentType = mimeLookup(filename) || 'application/octet-stream';
  const objectKey = `${externalId}/${filename}`;
  const bucket = supabase.storage.from(BUCKET);

  if (!DRY_RUN) {
    const { error } = await bucket.upload(objectKey, file, {
      upsert: true,
      contentType: String(contentType),
    });
    if (error) throw new Error(error.message);
  }

  const { data } = bucket.getPublicUrl(objectKey);
  return data.publicUrl;
}

async function main() {
  const raw = await fs.readFile(CONTENT_FILE, 'utf8');
  const listings = JSON.parse(raw) as ListingSource[];

  for (const l of listings) {
    const photoUrls: string[] = [];
    for (const filename of l.photos ?? []) {
      photoUrls.push(await uploadPhoto(l.external_id, filename));
    }

    const row = {
      external_id: l.external_id,
      title: l.title,
      address: l.address ?? null,
      price: l.price ?? null,
      psf: l.psf ?? null,
      size: l.size ?? null,
      property_type: l.property_type ?? null,
      status: l.status ?? 'Active',
      distance: l.distance ?? null,
      link: l.link ?? null,
      listed_at: l.listed_at ?? null,
      photos: photoUrls,
    };

    if (DRY_RUN) {
      console.log('[DRY_RUN] upsert listing', row.external_id, row);
      continue;
    }

    const { error } = await supabase.from('listings').upsert(row, {
      onConflict: 'external_id',
    });
    if (error) throw new Error(error.message);
    console.log(`Upserted listing: ${row.external_id}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});


