import { Inject, Injectable } from '@nestjs/common';
import type { SupabaseClient } from '@supabase/supabase-js';
import { SUPABASE_CLIENT } from '../supabase/supabase.constants';
import type {
  SaleTransaction,
  ListSaleTransactionsResponse,
  UpsertSaleTransactionRequest,
  UpsertSaleTransactionResponse,
  ListListingsResponse,
  Listing,
  UpsertListingRequest,
  UpsertListingResponse,
} from './admin.types';

function asIsoDate(s: unknown): string | null {
  if (typeof s !== 'string') return null;
  const t = s.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(t)) return null;
  return t;
}

function asNum(n: unknown): number | null {
  if (n == null) return null;
  const v = typeof n === 'string' ? Number(n) : typeof n === 'number' ? n : NaN;
  if (!Number.isFinite(v)) return null;
  return v;
}

function computeAnnualisedPct({
  purchasePrice,
  salePrice,
  purchaseDate,
  saleDate,
}: {
  purchasePrice: number;
  salePrice: number;
  purchaseDate: string; // YYYY-MM-DD
  saleDate: string; // YYYY-MM-DD
}) {
  const start = new Date(purchaseDate);
  const end = new Date(saleDate);
  const days = (end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24);
  if (!Number.isFinite(days) || days <= 0) return null;
  const cagr = Math.pow(salePrice / purchasePrice, 365 / days) - 1;
  return cagr * 100;
}

@Injectable()
export class AdminService {
  constructor(@Inject(SUPABASE_CLIENT) private readonly supabase: SupabaseClient) {}

  async requireLoggedInUserEmail(authHeader: string | undefined) {
    const h = (authHeader ?? '').trim();
    const token = h.toLowerCase().startsWith('bearer ') ? h.slice(7).trim() : '';
    if (!token) return null;
    const url = process.env.SUPABASE_URL;
    const apiKey = process.env.SUPABASE_API_KEY;
    if (!url || !apiKey) return null;
    try {
      const res = await fetch(`${url.replace(/\/$/, '')}/auth/v1/user`, {
        headers: {
          apikey: apiKey,
          authorization: `Bearer ${token}`,
        },
      });
      if (!res.ok) return null;
      const data = (await res.json()) as { email?: unknown };
      const email = data?.email;
      return typeof email === 'string' && email ? email : null;
    } catch {
      return null;
    }
  }

  async listCondoNames() {
    const { data, error } = await this.supabase
      .from('sale_transactions')
      .select('condo_name')
      .order('condo_name', { ascending: true })
      .limit(1000);

    if (error) return { ok: false as const, error: error.message };
    const names = (data ?? [])
      .map((r) => (r as { condo_name?: unknown })?.condo_name)
      .filter((x): x is string => typeof x === 'string' && x.trim().length > 0)
      .map((x) => x.trim());
    const uniq = Array.from(new Set(names));
    return { ok: true as const, condos: uniq };
  }

  async listSaleTransactions({
    condo,
    limit,
    offset,
  }: {
    condo?: string;
    limit?: number;
    offset?: number;
  }): Promise<ListSaleTransactionsResponse> {
    const condoName = (condo ?? '').trim();

    const safeLimit = Math.min(100, Math.max(1, Number.isFinite(limit ?? NaN) ? (limit as number) : 20));
    const safeOffset = Math.max(0, Number.isFinite(offset ?? NaN) ? (offset as number) : 0);

    const q = this.supabase
      .from('sale_transactions')
      .select('*', { count: 'exact' })
      .order('sale_date', { ascending: false })
      .order('created_at', { ascending: false })
      .range(safeOffset, safeOffset + safeLimit - 1);

    const { data, error, count } = condoName
      ? await q.eq('condo_name', condoName)
      : await q;
    if (error) return { ok: false, error: error.message };

    // GUARDRAIL: Never expose exact_unit in API responses
    const safeRows = (data ?? []).map((row) => {
      const safe = { ...row } as Record<string, unknown>;
      delete safe.exact_unit;
      return safe as SaleTransaction;
    });

    return {
      ok: true,
      rows: safeRows,
      total_count: typeof count === 'number' ? count : 0,
      limit: safeLimit,
      offset: safeOffset,
    };
  }

  async upsertSaleTransaction(
    body: UpsertSaleTransactionRequest,
  ): Promise<UpsertSaleTransactionResponse> {
    const id = typeof body.id === 'string' && body.id.trim() ? body.id.trim() : undefined;
    let base: Partial<SaleTransaction> = {};
    if (id) {
      const { data } = await this.supabase
        .from('sale_transactions')
        .select('*')
        .eq('id', id)
        .maybeSingle();
      base = (data ?? {}) as Partial<SaleTransaction>;
    }

    const merged = {
      ...base,
      ...body,
      id: id ?? (base.id as string | undefined),
    } as Partial<SaleTransaction> & {
      condo_name?: unknown;
      property_type?: unknown;
      unit_type?: unknown;
      sqft?: unknown;
      exact_level?: unknown;
      exact_unit?: unknown;
      level_low?: unknown;
      level_high?: unknown;
      purchase_date?: unknown;
      purchase_price?: unknown;
      sale_date?: unknown;
      sale_price?: unknown;
    };

    const condo_name =
      typeof merged.condo_name === 'string' && merged.condo_name.trim()
        ? merged.condo_name.trim()
        : null;
    const property_type =
      typeof merged.property_type === 'string' && merged.property_type.trim()
        ? merged.property_type.trim()
        : null;
    const unit_type =
      typeof merged.unit_type === 'string' && merged.unit_type.trim()
        ? merged.unit_type.trim()
        : null;
    const sqft = asNum(merged.sqft);
    const exact_level = asNum(merged.exact_level);
    const exact_unit = merged.exact_unit != null ? String(merged.exact_unit).trim() : null;
    const level_low = asNum(merged.level_low);
    const level_high = asNum(merged.level_high);
    const purchase_date =
      merged.purchase_date == null || merged.purchase_date === ''
        ? null
        : asIsoDate(merged.purchase_date);
    const sale_date =
      merged.sale_date == null || merged.sale_date === ''
        ? null
        : asIsoDate(merged.sale_date);
    const purchase_price = asNum(merged.purchase_price);
    const sale_price = asNum(merged.sale_price);

    if (!condo_name) return { ok: false, error: 'condo_name is required.' };
    if (!unit_type) return { ok: false, error: 'unit_type is required.' };
    if (merged.purchase_date != null && merged.purchase_date !== '' && !purchase_date) {
      return { ok: false, error: 'purchase_date must be YYYY-MM-DD.' };
    }
    if (merged.sale_date != null && merged.sale_date !== '' && !sale_date) {
      return { ok: false, error: 'sale_date must be YYYY-MM-DD.' };
    }
    if (sqft != null && sqft <= 0) {
      return { ok: false, error: 'sqft must be a positive number.' };
    }
    if (purchase_price != null && purchase_price <= 0) {
      return { ok: false, error: 'purchase_price must be a positive number.' };
    }
    if (sale_price != null && sale_price <= 0) {
      return { ok: false, error: 'sale_price must be a positive number.' };
    }

    const canCompute =
      purchase_price != null &&
      sale_price != null &&
      purchase_price > 0 &&
      sale_price > 0 &&
      purchase_date != null &&
      sale_date != null;

    const profit = canCompute ? sale_price - purchase_price : null;
    const annualised_pct = canCompute
      ? computeAnnualisedPct({
          purchasePrice: purchase_price,
          salePrice: sale_price,
          purchaseDate: purchase_date,
          saleDate: sale_date,
        })
      : null;

    const row = {
      ...(id ? { id } : {}),
      condo_name,
      property_type,
      unit_type,
      sqft,
      exact_level,
      exact_unit, // stored but NEVER returned
      level_low,
      level_high,
      purchase_date,
      purchase_price,
      sale_date,
      sale_price,
      profit,
      annualised_pct,
    };

    const { data, error } = await this.supabase
      .from('sale_transactions')
      .upsert(row)
      .select('*')
      .single();

    if (error) return { ok: false, error: error.message };

    // GUARDRAIL: Never expose exact_unit in API responses
    const safeRow = { ...data } as Record<string, unknown>;
    delete safeRow.exact_unit;

    return { ok: true, row: safeRow as SaleTransaction };
  }

  async listListings({
    limit,
    offset,
  }: {
    limit?: number;
    offset?: number;
  }): Promise<ListListingsResponse> {
    const safeLimit = Math.min(
      100,
      Math.max(1, Number.isFinite(limit ?? NaN) ? (limit as number) : 25),
    );
    const safeOffset = Math.max(0, Number.isFinite(offset ?? NaN) ? (offset as number) : 0);

    const { data, error, count } = await this.supabase
      .from('listings')
      .select('*', { count: 'exact' })
      .order('listed_at', { ascending: false, nullsFirst: false })
      .order('created_at', { ascending: false })
      .range(safeOffset, safeOffset + safeLimit - 1);

    if (error) return { ok: false, error: error.message };

    const rows = (data ?? []).map((r) => {
      const x = r as any;
      const photosRaw = x?.photos;
      const photos = Array.isArray(photosRaw)
        ? photosRaw.filter((p: unknown) => typeof p === 'string' && p.trim()).map((p: string) => p.trim())
        : [];
      return { ...x, photos } as Listing;
    });

    return {
      ok: true,
      rows,
      total_count: typeof count === 'number' ? count : 0,
      limit: safeLimit,
      offset: safeOffset,
    };
  }

  async upsertListing(body: UpsertListingRequest): Promise<UpsertListingResponse> {
    const external_id =
      typeof body.external_id === 'string' && body.external_id.trim()
        ? body.external_id.trim()
        : null;
    if (!external_id) return { ok: false, error: 'external_id is required.' };

    const id = typeof body.id === 'string' && body.id.trim() ? body.id.trim() : undefined;

    let base: Partial<Listing> = {};
    if (id) {
      const { data } = await this.supabase
        .from('listings')
        .select('*')
        .eq('id', id)
        .maybeSingle();
      base = (data ?? {}) as Partial<Listing>;
    } else {
      // If id not provided, attempt to find existing row by external_id
      const { data } = await this.supabase
        .from('listings')
        .select('*')
        .eq('external_id', external_id)
        .maybeSingle();
      base = (data ?? {}) as Partial<Listing>;
    }

    const pickStr = (v: unknown) => (typeof v === 'string' ? v.trim() : '');
    const asNullableStr = (v: unknown) => {
      if (v == null) return null;
      const s = pickStr(v);
      return s ? s : null;
    };

    const photos = Array.isArray(body.photos)
      ? body.photos
          .filter((p) => typeof p === 'string' && p.trim())
          .map((p) => p.trim())
      : (base.photos as unknown as string[] | undefined) ?? [];

    const listed_at = body.listed_at == null ? base.listed_at ?? null : asNullableStr(body.listed_at);
    if (listed_at != null && !/^\d{4}-\d{2}-\d{2}$/.test(listed_at)) {
      return { ok: false, error: 'listed_at must be YYYY-MM-DD.' };
    }

    const row = {
      ...(base.id ? { id: base.id } : {}),
      external_id,
      title: pickStr(body.title ?? base.title) || external_id,
      address: asNullableStr(body.address ?? base.address),
      price: asNullableStr(body.price ?? base.price),
      psf: asNullableStr(body.psf ?? base.psf),
      size: asNullableStr(body.size ?? base.size),
      property_type: asNullableStr(body.property_type ?? base.property_type),
      status: pickStr(body.status ?? base.status) || 'Draft',
      distance: asNullableStr(body.distance ?? base.distance),
      link: asNullableStr(body.link ?? base.link),
      listed_at,
      photos,
    };

    const { data, error } = await this.supabase
      .from('listings')
      .upsert(row)
      .select('*')
      .single();

    if (error) return { ok: false, error: error.message };
    const out = data as any;
    return {
      ok: true,
      row: {
        ...out,
        photos: Array.isArray(out.photos)
          ? out.photos.filter((p: unknown) => typeof p === 'string' && p.trim())
          : [],
      } as Listing,
    };
  }
}
