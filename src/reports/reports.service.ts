import fs from 'node:fs/promises';
import path from 'node:path';
import { Inject, Injectable } from '@nestjs/common';
import { validate as validateEmail } from '@mailtester/core';
import type { SupabaseClient } from '@supabase/supabase-js';
import * as postmark from 'postmark';
import { LeadsService } from '../leads/leads.service';
import { SUPABASE_CLIENT } from '../supabase/supabase.constants';
import type {
  CondoSaleProfitabilityResponse,
  CondoSaleProfitabilityByYearResponse,
  CondoSaleProfitabilityRowsResponse,
  SendMarketOutlook2026Request,
} from './reports.types';

@Injectable()
export class ReportsService {
  constructor(
    private readonly leads: LeadsService,
    @Inject(SUPABASE_CLIENT) private readonly supabase: SupabaseClient,
  ) {}

  private median(values: number[]) {
    if (!values.length) return null;
    const sorted = [...values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 1) return sorted[mid];
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }

  async getCondoSaleProfitability(
    condo?: string,
  ): Promise<CondoSaleProfitabilityResponse> {
    const condoName = (condo ?? '').trim();
    if (!condoName) return { ok: false, error: 'Missing condo parameter.' };

    const { data, error } = await this.supabase
      .from('sale_transactions')
      .select(
        'condo_name, purchase_price, sale_price, annualised_pct, purchase_date, sale_date',
      )
      .eq('condo_name', condoName);

    if (error) return { ok: false, error: error.message };
    const rows = (data ?? []) as Array<{
      condo_name: string;
      purchase_price: unknown;
      sale_price: unknown;
      annualised_pct: unknown;
      purchase_date: unknown;
      sale_date: unknown;
    }>;

    const profitabilityPcts: number[] = [];
    const annualisedPcts: number[] = [];

    for (const r of rows) {
      const purchasePrice =
        r.purchase_price == null ? NaN : Number(r.purchase_price);
      const salePrice = r.sale_price == null ? NaN : Number(r.sale_price);
      const annualised =
        r.annualised_pct == null ? NaN : Number(r.annualised_pct);

      if (
        Number.isFinite(purchasePrice) &&
        purchasePrice > 0 &&
        Number.isFinite(salePrice) &&
        salePrice > 0
      ) {
        profitabilityPcts.push(((salePrice - purchasePrice) / purchasePrice) * 100);
      }

      if (Number.isFinite(annualised)) {
        annualisedPcts.push(annualised);
      } else {
        // Fallback if annualised_pct isn't present: compute CAGR from dates/prices.
        const salePrice = Number(r.sale_price);
        const purchaseDate =
          typeof r.purchase_date === 'string' ? new Date(r.purchase_date) : null;
        const saleDate =
          typeof r.sale_date === 'string' ? new Date(r.sale_date) : null;
        if (
          Number.isFinite(purchasePrice) &&
          Number.isFinite(salePrice) &&
          purchasePrice > 0 &&
          salePrice > 0 &&
          purchaseDate &&
          saleDate &&
          Number.isFinite(purchaseDate.getTime()) &&
          Number.isFinite(saleDate.getTime())
        ) {
          const days = (saleDate.getTime() - purchaseDate.getTime()) / (1000 * 60 * 60 * 24);
          if (days > 0) {
            const cagr = Math.pow(salePrice / purchasePrice, 365 / days) - 1;
            annualisedPcts.push(cagr * 100);
          }
        }
      }
    }

    const medianProfitability = this.median(profitabilityPcts);
    const medianAnnualised = this.median(annualisedPcts);

    if (medianProfitability === null || medianAnnualised === null) {
      return { ok: false, error: 'No valid transactions found.' };
    }

    return {
      ok: true,
      condo_name: condoName,
      transaction_count: rows.length,
      median_profitability_pct: medianProfitability,
      median_annualised_pct: medianAnnualised,
    };
  }

  async getCondoSaleProfitabilityByYear(
    condosCsv?: string,
  ): Promise<CondoSaleProfitabilityByYearResponse> {
    const condos = (condosCsv ?? '')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
      .slice(0, 30);

    if (!condos.length) return { ok: false, error: 'Missing condos parameter.' };

    const rows: Array<{
      condo_name: unknown;
      purchase_price: unknown;
      sale_price: unknown;
      sale_date: unknown;
    }> = [];
    const PAGE_SIZE = 1000;
    for (let offset = 0; ; offset += PAGE_SIZE) {
      const { data, error } = await this.supabase
        .from('sale_transactions')
        .select('condo_name, purchase_price, sale_price, sale_date')
        .in('condo_name', condos)
        .range(offset, offset + PAGE_SIZE - 1);
      if (error) return { ok: false, error: error.message };
      const batch = (data ?? []) as Array<{
        condo_name: unknown;
        purchase_price: unknown;
        sale_price: unknown;
        sale_date: unknown;
      }>;
      rows.push(...batch);
      if (batch.length < PAGE_SIZE) break;
    }

    // condo -> year -> profit%[]
    const bucket = new Map<string, Map<number, number[]>>();
    const yearSet = new Set<number>();

    for (const r of rows) {
      const condoName = typeof r.condo_name === 'string' ? r.condo_name : '';
      if (!condoName) continue;

      const purchasePrice =
        r.purchase_price == null ? NaN : Number(r.purchase_price);
      const salePrice = r.sale_price == null ? NaN : Number(r.sale_price);

      const saleDate =
        typeof r.sale_date === 'string' ? new Date(r.sale_date) : null;
      const saleYear =
        saleDate && Number.isFinite(saleDate.getTime()) ? saleDate.getFullYear() : null;

      if (
        saleYear === null ||
        !Number.isFinite(purchasePrice) ||
        purchasePrice <= 0 ||
        !Number.isFinite(salePrice) ||
        salePrice <= 0
      ) {
        continue;
      }

      const pct = ((salePrice - purchasePrice) / purchasePrice) * 100;
      yearSet.add(saleYear);

      if (!bucket.has(condoName)) bucket.set(condoName, new Map());
      const byYear = bucket.get(condoName)!;
      if (!byYear.has(saleYear)) byYear.set(saleYear, []);
      byYear.get(saleYear)!.push(pct);
    }

    const years = [...yearSet].sort((a, b) => a - b);
    if (!years.length) return { ok: false, error: 'No valid transactions found.' };

    const series = condos.map((condo) => {
      const byYear = bucket.get(condo) ?? new Map<number, number[]>();
      const points = years.map((year) => {
        const arr = byYear.get(year) ?? [];
        return {
          year,
          transaction_count: arr.length,
          median_profitability_pct: this.median(arr),
        };
      });
      return { condo_name: condo, points };
    });

    return { ok: true, condos, years, series };
  }

  async getCondoSaleProfitabilityRows(
    condosCsv?: string,
  ): Promise<CondoSaleProfitabilityRowsResponse> {
    const condos = (condosCsv ?? '')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
      .slice(0, 30);

    if (!condos.length) return { ok: false, error: 'Missing condos parameter.' };

    const rows: Array<{
      condo_name: unknown;
      unit_type: unknown;
      purchase_price: unknown;
      sale_price: unknown;
      sale_date: unknown;
    }> = [];
    const PAGE_SIZE = 1000;
    for (let offset = 0; ; offset += PAGE_SIZE) {
      const { data, error } = await this.supabase
        .from('sale_transactions')
        .select('condo_name, unit_type, purchase_price, sale_price, sale_date')
        .in('condo_name', condos)
        .range(offset, offset + PAGE_SIZE - 1);
      if (error) return { ok: false, error: error.message };
      const batch = (data ?? []) as Array<{
        condo_name: unknown;
        unit_type: unknown;
        purchase_price: unknown;
        sale_price: unknown;
        sale_date: unknown;
      }>;
      rows.push(...batch);
      if (batch.length < PAGE_SIZE) break;
    }

    const out: Array<{
      condo_name: string;
      unit_type: string | null;
      sale_month: string;
      profitability_pct: number;
    }> = [];

    for (const r of rows) {
      const condoName = typeof r.condo_name === 'string' ? r.condo_name : '';
      if (!condoName) continue;

      const unitType = typeof r.unit_type === 'string' ? r.unit_type : null;
      const purchasePrice =
        r.purchase_price == null ? NaN : Number(r.purchase_price);
      const salePrice = r.sale_price == null ? NaN : Number(r.sale_price);

      const saleDate =
        typeof r.sale_date === 'string' ? new Date(r.sale_date) : null;
      const saleMonth =
        saleDate && Number.isFinite(saleDate.getTime())
          ? `${saleDate.getFullYear()}-${String(saleDate.getMonth() + 1).padStart(2, '0')}`
          : null;

      if (
        saleMonth === null ||
        !Number.isFinite(purchasePrice) ||
        purchasePrice <= 0 ||
        !Number.isFinite(salePrice) ||
        salePrice <= 0
      ) {
        continue;
      }

      out.push({
        condo_name: condoName,
        unit_type: unitType,
        sale_month: saleMonth,
        profitability_pct: ((salePrice - purchasePrice) / purchasePrice) * 100,
      });
    }

    if (!out.length) return { ok: false, error: 'No valid transactions found.' };
    return { ok: true, condos, rows: out };
  }

  sendMarketOutlook2026(body: SendMarketOutlook2026Request) {
    const email = (body.email ?? '').trim().toLowerCase();
    if (!email) return { ok: false as const, error: 'Email is required.' };

    const postmarkKey = process.env.POSTMARK_API_KEY;
    const from = process.env.POSTMARK_FROM;
    if (!postmarkKey || !from) {
      console.log('[Reports] Missing Postmark env vars', {
        hasPostmarkKey: Boolean(postmarkKey),
        hasFrom: Boolean(from),
      });
      return { ok: false as const, error: 'Email service not configured.' };
    }

    // Minimal synchronous validation only (background job does deeper validation).
    // This endpoint should return quickly so the UI can show "Sent!" immediately.
    const basicEmailOk = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
    if (!basicEmailOk) {
      return { ok: false as const, error: 'Invalid email.' };
    }

    // Fire-and-forget background work (do not await).
    this.runBackgroundSendMarketOutlook2026({
      email,
      utm: body.utm,
      context: body.context,
    }).catch((err) => {
      const e = err as { message?: unknown };
      console.log('[Reports] Background send failed', {
        email,
        message: typeof e?.message === 'string' ? e.message : String(err),
      });
    });

    console.log('[Reports] Accepted report request (background send started)', {
      email,
    });

    return { ok: true as const, message: 'Sending report.' };
  }

  private async runBackgroundSendMarketOutlook2026(
    body: SendMarketOutlook2026Request,
  ) {
    const email = (body.email ?? '').trim().toLowerCase();

    const postmarkKey = process.env.POSTMARK_API_KEY;
    const from = process.env.POSTMARK_FROM;
    if (!postmarkKey || !from) {
      throw new Error('Missing POSTMARK_API_KEY or POSTMARK_FROM');
    }

    console.log('[Reports] Background send entered', { email });

    // Deeper email validation (still bounded).
    const validation = await validateEmail(email, {
      preset: 'balanced',
      earlyExit: true,
      timeout: 3500,
      validators: { smtp: { enabled: false } },
    });
    const reason = validation.reason ?? 'invalid';
    if (!validation.valid && reason !== 'disposable') {
      console.log('[Reports] Email validation failed', { email, reason });
      return;
    }

    // Lead capture (store attribution/context).
    await this.leads.createLead({
      email,
      utm: body.utm,
      context: body.context,
    });

    const pdfPath = path.resolve(
      process.cwd(),
      'content/assets/marketoutlook2026.pdf',
    );
    const pdf = await fs.readFile(pdfPath);

    const subject = 'Singapore Market Outlook 2026 (Report)';
    const logoUrl = process.env.PUBLIC_ASSETS_BASE_URL
      ? `${process.env.PUBLIC_ASSETS_BASE_URL.replace(/\/$/, '')}/hart-logo.png`
      : 'https://hartproperty.sg/hart-logo.png';

    const textBody =
      `Here is the Singapore Market Outlook 2026 report (PDF attached).\n\n` +
      `You received this because you requested it on hartproperty.sg.\n` +
      `If this wasn’t you, reply to this email.\n`;

    const htmlBody = `
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width" />
    <title>${subject}</title>
  </head>
  <body style="margin:0;padding:0;background:#F7F6F3;">
    <div style="max-width:640px;margin:0 auto;padding:24px;">
      <div style="background:#ffffff;border:1px solid rgba(62,92,138,0.15);border-radius:14px;overflow:hidden;">
        <div style="padding:22px 22px 14px 22px;border-bottom:1px solid rgba(62,92,138,0.15);">
          <div style="display:flex;align-items:center;">
            <img src="${logoUrl}" alt="Hart Property" width="44" height="44" style="display:block;margin-right:16px;" />
            <div style="font-family:Inter,Arial,sans-serif;">
              <div style="font-size:16px;font-weight:800;letter-spacing:-0.02em;color:#13305D;">HART PROPERTY</div>
              <div style="font-size:12px;color:#3E5C8A;">Singapore Market Outlook 2026</div>
            </div>
          </div>
        </div>

        <div style="padding:22px;font-family:Inter,Arial,sans-serif;color:#1F2933;line-height:1.55;">
          <div style="font-size:16px;font-weight:700;color:#13305D;margin-bottom:10px;">Your PDF report is attached</div>
          <div style="font-size:13px;color:#3E5C8A;">
            You received this because you requested it on
            <a href="https://hartproperty.sg" style="color:#4C7DBF;text-decoration:none;">hartproperty.sg</a>.
            If this wasn’t you, please reply to this email.
          </div>
        </div>

        <div style="padding:16px 22px;border-top:1px solid rgba(62,92,138,0.15);font-family:JetBrains Mono,ui-monospace,Menlo,Monaco,Consolas,monospace;font-size:11px;line-height:1.6;color:#3E5C8A;">
          <div>HART PROPERTY • Michael Hart | CEA Registration: R071893C | Agency License: L3008022J</div>
        </div>
      </div>
    </div>
  </body>
</html>
`.trim();

    const client = new postmark.ServerClient(postmarkKey);
    await client.sendEmail({
      MessageStream: 'outbound',
      From: `Hart Property <${from}>`,
      To: email,
      Bcc: 'michael.hart@hartproperty.sg',
      ReplyTo: `Hart Property <${from}>`,
      Subject: subject,
      TextBody: textBody,
      HtmlBody: htmlBody,
      Attachments: [
        {
          Name: 'Singapore-Market-Outlook-2026.pdf',
          Content: pdf.toString('base64'),
          ContentType: 'application/pdf',
          ContentID: null,
        },
      ],
    });

    console.log('[Reports] Background send completed', { email });
  }
}
