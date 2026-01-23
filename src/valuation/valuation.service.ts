import { Inject, Injectable } from '@nestjs/common';
import type { SupabaseClient } from '@supabase/supabase-js';
import * as postmark from 'postmark';
import { SUPABASE_CLIENT } from '../supabase/supabase.constants';
import type {
  ValuationRequest,
  ValuationResult,
  ValuationResponse,
} from './valuation.types';

@Injectable()
export class ValuationService {
  constructor(
    @Inject(SUPABASE_CLIENT) private readonly supabase: SupabaseClient,
  ) {}

  /**
   * Parse unit number to extract floor
   * Formats: "12-05", "#12-05", "1205", "12"
   */
  private parseFloor(unitNumber: string): number | null {
    const cleaned = unitNumber.replace(/^#/, '').trim();

    // Format: "12-05" or "12 - 05"
    const dashMatch = cleaned.match(/^(\d+)\s*-\s*\d+$/);
    if (dashMatch) {
      return parseInt(dashMatch[1], 10);
    }

    // Format: "1205" (4 digits, first 2 are floor)
    if (/^\d{4}$/.test(cleaned)) {
      return parseInt(cleaned.substring(0, 2), 10);
    }

    // Format: just a number (assume it's the floor)
    if (/^\d{1,2}$/.test(cleaned)) {
      return parseInt(cleaned, 10);
    }

    return null;
  }

  /**
   * Get floor range string from floor number
   */
  private getFloorRange(floor: number): { low: number; high: number; label: string } {
    if (floor <= 5) return { low: 1, high: 5, label: '01 to 05' };
    if (floor <= 10) return { low: 6, high: 10, label: '06 to 10' };
    if (floor <= 15) return { low: 11, high: 15, label: '11 to 15' };
    if (floor <= 20) return { low: 16, high: 20, label: '16 to 20' };
    if (floor <= 25) return { low: 21, high: 25, label: '21 to 25' };
    if (floor <= 30) return { low: 26, high: 30, label: '26 to 30' };
    if (floor <= 35) return { low: 31, high: 35, label: '31 to 35' };
    if (floor <= 40) return { low: 36, high: 40, label: '36 to 40' };
    return { low: 41, high: 99, label: '41+' };
  }

  /**
   * Format floor range for display (e.g., "01 to 05")
   */
  private formatFloorRange(low: number | null, high: number | null): string {
    if (low === null && high === null) return '?';
    if (low === high) return String(low).padStart(2, '0');
    const l = low !== null ? String(low).padStart(2, '0') : '?';
    const h = high !== null ? String(high).padStart(2, '0') : '?';
    return `${l} to ${h}`;
  }

  /**
   * Calculate months difference between two dates
   */
  private monthsDiff(d1: Date, d2: Date): number {
    return (
      (d2.getFullYear() - d1.getFullYear()) * 12 +
      (d2.getMonth() - d1.getMonth())
    );
  }

  /**
   * Calculate appreciation rate from transaction history using weighted regression
   * Recent transactions are weighted more heavily than older ones
   * Returns annual appreciation as decimal (e.g., 0.03 for 3%)
   */
  private calculateAppreciationRate(
    transactions: { sale_price: number; sale_date: string; sqft: number }[],
  ): number {
    if (transactions.length < 2) return 0.03; // Default 3% if insufficient data

    const today = new Date();

    // Calculate PSF and time for each transaction with recency weight
    const dataPoints: { yearsAgo: number; psf: number; weight: number }[] = [];
    for (const t of transactions) {
      const saleDate = new Date(t.sale_date);
      const monthsAgo = this.monthsDiff(saleDate, today);
      const yearsAgo = monthsAgo / 12;
      const psf = t.sale_price / t.sqft;

      // Weight: exponential decay - recent transactions weighted much more heavily
      // 6 months ago = ~0.90 weight, 1 year = ~0.82, 2 years = ~0.67, 3 years = ~0.55
      const weight = Math.pow(0.85, yearsAgo);

      dataPoints.push({ yearsAgo, psf, weight });
    }

    // Calculate weighted linear regression: PSF = a + b * yearsAgo
    // We want b (slope) which gives us PSF change per year
    let sumW = 0;
    let sumWX = 0;
    let sumWY = 0;
    let sumWXX = 0;
    let sumWXY = 0;

    for (const dp of dataPoints) {
      const x = -dp.yearsAgo; // Negative because we want appreciation forward, not backward
      const y = dp.psf;
      const w = dp.weight;

      sumW += w;
      sumWX += w * x;
      sumWY += w * y;
      sumWXX += w * x * x;
      sumWXY += w * x * y;
    }

    // Weighted least squares: b = (sumW * sumWXY - sumWX * sumWY) / (sumW * sumWXX - sumWX^2)
    const denominator = sumW * sumWXX - sumWX * sumWX;
    if (Math.abs(denominator) < 0.0001) return 0.03; // Avoid division by zero

    const slope = (sumW * sumWXY - sumWX * sumWY) / denominator;
    const intercept = (sumWY - slope * sumWX) / sumW;

    // Current PSF estimate (at x=0, i.e., today)
    const currentPsf = intercept;
    if (currentPsf <= 0) return 0.03;

    // Annual appreciation rate = slope / currentPsf
    const annualRate = slope / currentPsf;

    // Clamp to reasonable range (-10% to +15%)
    return Math.max(-0.1, Math.min(0.15, annualRate));
  }

  /**
   * Determine unit type (e.g., "2BR", "3BR") from sqft by looking at DB
   */
  private async determineUnitType(
    condoName: string,
    sqft: number,
  ): Promise<string | null> {
    // Get unit types and their sqft ranges for this condo
    const { data } = await this.supabase
      .from('condo_sale_transactions')
      .select('unit_type, sqft')
      .ilike('condo_name', condoName)
      .not('unit_type', 'is', null)
      .not('sqft', 'is', null);

    if (!data || data.length === 0) return null;

    // Group by unit_type and find sqft range for each
    const typeRanges: Record<string, { min: number; max: number }> = {};
    for (const t of data) {
      const type = t.unit_type as string;
      const s = t.sqft as number;
      if (!typeRanges[type]) {
        typeRanges[type] = { min: s, max: s };
      } else {
        typeRanges[type].min = Math.min(typeRanges[type].min, s);
        typeRanges[type].max = Math.max(typeRanges[type].max, s);
      }
    }

    // Find which unit type this sqft falls into (with ±5% tolerance)
    for (const [type, range] of Object.entries(typeRanges)) {
      const tolerance = (range.max - range.min) * 0.1 || 50; // 10% of range or 50sqft min
      if (sqft >= range.min - tolerance && sqft <= range.max + tolerance) {
        return type;
      }
    }

    return null;
  }

  /**
   * Calculate valuation based on URA data with time-adjusted comparables
   */
  async calculateValuation(
    condoName: string,
    unitNumber: string,
    unitSqft: number,
  ): Promise<ValuationResult | null> {
    const floor = this.parseFloor(unitNumber);
    if (!floor) return null;

    const floorRange = this.getFloorRange(floor);
    const today = new Date();

    // Define sqft range for similar units (±15%) - use integers for DB query
    const sqftLow = Math.floor(unitSqft * 0.85);
    const sqftHigh = Math.ceil(unitSqft * 1.15);

    // First, get ALL transactions for this sqft range (no time limit) to find same-floor data
    const { data: allTimeTxns, error: allTimeError } = await this.supabase
      .from('condo_sale_transactions')
      .select('sale_price, sale_date, level_low, level_high, sqft, unit_type')
      .ilike('condo_name', condoName)
      .gte('sqft', sqftLow)
      .lte('sqft', sqftHigh)
      .not('sale_price', 'is', null)
      .not('sqft', 'is', null)
      .order('sale_date', { ascending: false });

    if (allTimeError || !allTimeTxns || allTimeTxns.length === 0) {
      return null;
    }

    // Filter to same floor range
    const sameFloorAllTime = allTimeTxns.filter(
      (t) =>
        t.level_low !== null &&
        t.level_high !== null &&
        t.level_low <= floorRange.high &&
        t.level_high >= floorRange.low,
    );

    // Get recent transactions (last 3 years) for appreciation calculation
    const threeYearsAgo = new Date();
    threeYearsAgo.setFullYear(threeYearsAgo.getFullYear() - 3);

    const recentTxnsForAppreciation = allTimeTxns.filter((t) => {
      const saleDate = new Date(t.sale_date as string);
      return saleDate >= threeYearsAgo;
    });

    // Calculate market appreciation rate from recent transactions of similar size
    const txnsForAppreciation =
      recentTxnsForAppreciation.length >= 3 ? recentTxnsForAppreciation : allTimeTxns;
    const appreciationRate = this.calculateAppreciationRate(
      txnsForAppreciation.map((t) => ({
        sale_price: t.sale_price as number,
        sale_date: t.sale_date as string,
        sqft: t.sqft as number,
      })),
    );

    // Priority: same floor transactions (even if older), then any similar sqft
    // Use same-floor if we have at least 1, otherwise fall back to similar sqft
    const allMatchingTxns =
      sameFloorAllTime.length >= 1 ? sameFloorAllTime : allTimeTxns;

    // Split into recent (<2 years) and older (2+ years) transactions
    const twoYearsAgo = new Date();
    twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);

    const recentOnlyTxns = allMatchingTxns.filter((t) => {
      const saleDate = new Date(t.sale_date as string);
      return saleDate >= twoYearsAgo;
    });

    const monthlyRate = appreciationRate / 12;

    let estimatedPsfMid: number;
    let estimatedPsfLow: number;
    let estimatedPsfHigh: number;

    if (recentOnlyTxns.length >= 3) {
      // Enough recent data - use only recent transactions with weighted average
      const recentPsfs: { psf: number; weight: number }[] = recentOnlyTxns.map((t) => {
        const saleDate = new Date(t.sale_date as string);
        const monthsAgo = this.monthsDiff(saleDate, today);
        const rawPsf = (t.sale_price as number) / (t.sqft as number);
        // Weight: exponential decay - recent transactions weighted more heavily
        const weight = Math.pow(0.92, monthsAgo); // ~8% decay per month
        return { psf: rawPsf, weight };
      });

      // Weighted average
      let weightedSum = 0;
      let weightSum = 0;
      for (const { psf, weight } of recentPsfs) {
        weightedSum += psf * weight;
        weightSum += weight;
      }
      estimatedPsfMid = Math.round(weightedSum / weightSum);

      // Low/high from recent transactions
      const sortedPsfs = recentPsfs.map((p) => p.psf).sort((a, b) => a - b);
      estimatedPsfLow = Math.round(sortedPsfs[0]);
      estimatedPsfHigh = Math.round(sortedPsfs[sortedPsfs.length - 1]);
    } else {
      // Limited recent data - use all data with appreciation adjustment
      const allPsfs: { psf: number; adjustedPsf: number; weight: number }[] =
        allMatchingTxns.map((t) => {
          const saleDate = new Date(t.sale_date as string);
          const monthsAgo = this.monthsDiff(saleDate, today);
          const rawPsf = (t.sale_price as number) / (t.sqft as number);
          // Adjust older transactions forward using appreciation rate
          const adjustedPsf = rawPsf * Math.pow(1 + monthlyRate, monthsAgo);
          // Weight: heavier decay for older transactions
          const weight = Math.pow(0.90, monthsAgo); // ~10% decay per month
          return { psf: rawPsf, adjustedPsf, weight };
        });

      // Weighted average of adjusted PSFs
      let weightedSum = 0;
      let weightSum = 0;
      for (const { adjustedPsf, weight } of allPsfs) {
        weightedSum += adjustedPsf * weight;
        weightSum += weight;
      }
      estimatedPsfMid = Math.round(weightedSum / weightSum);

      // Low/high from adjusted PSFs
      const sortedAdjusted = allPsfs
        .map((p) => p.adjustedPsf)
        .sort((a, b) => a - b);
      estimatedPsfLow = Math.round(sortedAdjusted[0]);
      estimatedPsfHigh = Math.round(sortedAdjusted[sortedAdjusted.length - 1]);
    }

    // Store which transactions we used for display
    const finalTxns = recentOnlyTxns.length >= 3 ? recentOnlyTxns : allMatchingTxns;

    // Apply floor adjustment if using different floor data
    if (sameFloorAllTime.length < 1 && finalTxns.length > 0) {
      const avgFloorInData =
        finalTxns.reduce((sum, t) => {
          const mid = ((t.level_low ?? 10) + (t.level_high ?? 10)) / 2;
          return sum + mid;
        }, 0) / finalTxns.length;

      const floorDiff = floor - avgFloorInData;
      const floorAdjustment = 1 + floorDiff * 0.005; // ~0.5% per floor

      estimatedPsfLow = Math.round(estimatedPsfLow * floorAdjustment);
      estimatedPsfMid = Math.round(estimatedPsfMid * floorAdjustment);
      estimatedPsfHigh = Math.round(estimatedPsfHigh * floorAdjustment);
    }

    // Calculate prices from PSF
    const estimatedPriceLow = Math.round(estimatedPsfLow * unitSqft);
    const estimatedPriceMid = Math.round(estimatedPsfMid * unitSqft);
    const estimatedPriceHigh = Math.round(estimatedPsfHigh * unitSqft);

    // Format recent transactions for display
    const recentTxns = finalTxns.slice(0, 10).map((t) => {
      const sqft = t.sqft as number | null;
      const price = t.sale_price as number;
      const rawPsf = sqft ? Math.round(price / sqft) : null;

      // Calculate time-adjusted PSF
      let adjustedPsf: number | null = null;
      if (sqft) {
        const saleDate = new Date(t.sale_date as string);
        const monthsAgo = this.monthsDiff(saleDate, today);
        adjustedPsf = Math.round(
          (price / sqft) * Math.pow(1 + monthlyRate, monthsAgo),
        );
      }

      return {
        sale_price: price,
        sale_date: t.sale_date as string,
        floor_range: this.formatFloorRange(
          t.level_low as number | null,
          t.level_high as number | null,
        ),
        sqft,
        psf: rawPsf,
        adjusted_psf: adjustedPsf,
      };
    });

    // Data period
    const dates = finalTxns.map((t) => new Date(t.sale_date as string));
    const oldestDate = new Date(Math.min(...dates.map((d) => d.getTime())));
    const newestDate = new Date(Math.max(...dates.map((d) => d.getTime())));
    const formatDate = (d: Date) =>
      d.toLocaleDateString('en-SG', { month: 'short', year: 'numeric' });

    return {
      condo_name: condoName,
      unit_number: unitNumber,
      unit_sqft: unitSqft,
      estimated_floor: floor,
      floor_range: floorRange.label,
      appreciation_rate: appreciationRate,
      estimated_psf_low: estimatedPsfLow,
      estimated_psf_mid: estimatedPsfMid,
      estimated_psf_high: estimatedPsfHigh,
      estimated_price_low: estimatedPriceLow,
      estimated_price_mid: estimatedPriceMid,
      estimated_price_high: estimatedPriceHigh,
      recent_transactions: recentTxns,
      data_period: `${formatDate(oldestDate)} - ${formatDate(newestDate)}`,
      generated_at: new Date().toISOString(),
    };
  }

  /**
   * Send valuation email to user and Michael
   */
  private async sendValuationEmail(
    email: string,
    name: string | undefined,
    valuation: ValuationResult,
  ): Promise<boolean> {
    const postmarkKey = process.env.POSTMARK_API_KEY;
    const from = process.env.POSTMARK_FROM;
    const michaelEmail = 'michael.hart@hartproperty.sg';

    if (!postmarkKey || !from) {
      console.log('[Valuation] Missing Postmark env vars');
      return false;
    }

    const client = new postmark.ServerClient(postmarkKey);

    const formatPrice = (n: number) =>
      `$${n.toLocaleString('en-SG', { maximumFractionDigits: 0 })}`;

    const greeting = name ? `Dear ${name}` : 'Hello';

    // Build transaction table
    const txnRows = valuation.recent_transactions
      .map(
        (t) =>
          `<tr>
            <td style="padding: 8px; border-bottom: 1px solid #eee;">${t.sale_date}</td>
            <td style="padding: 8px; border-bottom: 1px solid #eee;">${formatPrice(t.sale_price)}</td>
            <td style="padding: 8px; border-bottom: 1px solid #eee;">${t.floor_range}</td>
            <td style="padding: 8px; border-bottom: 1px solid #eee;">${t.sqft ?? '-'}</td>
            <td style="padding: 8px; border-bottom: 1px solid #eee;">${t.psf ? formatPrice(t.psf) : '-'}</td>
          </tr>`,
      )
      .join('');

    const htmlBody = `
      <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h1 style="color: #1a1a1a; font-size: 24px; margin-bottom: 24px;">
          Property Valuation Report
        </h1>
        
        <p style="color: #333; font-size: 16px; line-height: 1.6;">
          ${greeting},
        </p>
        
        <p style="color: #333; font-size: 16px; line-height: 1.6;">
          Thank you for using Hart Property's free valuation tool. Here's your estimated valuation for:
        </p>
        
        <div style="background: #f8f9fa; border-radius: 12px; padding: 20px; margin: 24px 0;">
          <h2 style="color: #1a1a1a; font-size: 20px; margin: 0 0 16px 0;">
            ${valuation.condo_name}
          </h2>
          <p style="color: #666; margin: 0 0 8px 0;">
            Unit: <strong>${valuation.unit_number}</strong> (Floor ${valuation.estimated_floor}, Range: ${valuation.floor_range})<br>
            Size: <strong>${valuation.unit_sqft} sqft</strong>
          </p>
        </div>
        
        <div style="background-color: #4F46E5; border-radius: 12px; padding: 24px; margin: 24px 0; color: white;">
          <h3 style="margin: 0 0 16px 0; font-size: 14px; text-transform: uppercase; letter-spacing: 1px; color: #E0E7FF;">
            Estimated Value Range
          </h3>
          <div style="font-size: 32px; font-weight: bold; margin-bottom: 8px; color: white;">
            ${formatPrice(valuation.estimated_price_mid)}
          </div>
          <div style="font-size: 14px; color: #E0E7FF;">
            Range: ${formatPrice(valuation.estimated_price_low)} - ${formatPrice(valuation.estimated_price_high)}
          </div>
        </div>
        
        <h3 style="color: #1a1a1a; font-size: 18px; margin: 32px 0 16px 0;">
          Recent Comparable Transactions
        </h3>
        <p style="color: #666; font-size: 14px; margin-bottom: 16px;">
          Based on ${valuation.recent_transactions.length} similar transactions from ${valuation.data_period}
        </p>
        
        <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
          <thead>
            <tr style="background: #f1f3f4;">
              <th style="padding: 12px 8px; text-align: left;">Date</th>
              <th style="padding: 12px 8px; text-align: left;">Price</th>
              <th style="padding: 12px 8px; text-align: left;">Floor</th>
              <th style="padding: 12px 8px; text-align: left;">Sqft</th>
              <th style="padding: 12px 8px; text-align: left;">PSF</th>
            </tr>
          </thead>
          <tbody>
            ${txnRows}
          </tbody>
        </table>
        
        <div style="background: #fff3cd; border-radius: 8px; padding: 16px; margin: 24px 0; border-left: 4px solid #ffc107;">
          <p style="color: #856404; margin: 0; font-size: 14px;">
            <strong>Disclaimer:</strong> This is an automated estimate based on URA transaction data. 
            Actual value may vary based on unit condition, facing, renovations, and current market conditions.
          </p>
        </div>
        
        <div style="background: #e8f4f8; border-radius: 12px; padding: 20px; margin: 24px 0;">
          <h3 style="color: #1a1a1a; margin: 0 0 12px 0; font-size: 16px;">
            Need a professional review?
          </h3>
          <p style="color: #333; margin: 0 0 16px 0; font-size: 14px; line-height: 1.6;">
            I can provide a more detailed valuation with access to banker valuations and current market listings. 
            Whether you're looking to sell or just curious about your options, I'm happy to help.
          </p>
          <a href="https://hartproperty.sg/buyer-advisory" 
             style="display: inline-block; background: #1a1a1a; color: white; padding: 12px 24px; border-radius: 8px; text-decoration: none; font-weight: 500;">
            Contact Michael Hart
          </a>
        </div>
        
        <p style="color: #666; font-size: 14px; margin-top: 32px;">
          Best regards,<br>
          <strong>Michael Hart</strong><br>
          Hart Property<br>
          <a href="mailto:michael.hart@hartproperty.sg" style="color: #667eea;">michael.hart@hartproperty.sg</a>
        </p>
      </div>
    `;

    const textBody = `
Property Valuation Report for ${valuation.condo_name}

${greeting},

Thank you for using Hart Property's free valuation tool.

Unit: ${valuation.unit_number} (Floor ${valuation.estimated_floor}, ${valuation.unit_sqft} sqft)

ESTIMATED VALUE RANGE
Mid estimate: ${formatPrice(valuation.estimated_price_mid)}
Range: ${formatPrice(valuation.estimated_price_low)} - ${formatPrice(valuation.estimated_price_high)}

Based on ${valuation.recent_transactions.length} comparable transactions from ${valuation.data_period}.

Disclaimer: This is an automated estimate based on URA transaction data. Actual value may vary based on unit condition, facing, renovations, and current market conditions.

Need a professional review? Contact me for access to banker valuations and current market listings.

Best regards,
Michael Hart
Hart Property
michael.hart@hartproperty.sg
    `;

    try {
      // Send to user
      await client.sendEmail({
        From: from,
        To: email,
        Subject: `Property Valuation: ${valuation.condo_name} ${valuation.unit_number}`,
        HtmlBody: htmlBody,
        TextBody: textBody,
      });

      // Send copy to Michael
      await client.sendEmail({
        From: from,
        To: michaelEmail,
        Subject: `[Valuation Lead] ${valuation.condo_name} ${valuation.unit_number} - ${email}`,
        HtmlBody: `
          <p><strong>New valuation request from:</strong> ${email}${name ? ` (${name})` : ''}</p>
          <hr>
          ${htmlBody}
        `,
        TextBody: `New valuation request from: ${email}${name ? ` (${name})` : ''}\n\n${textBody}`,
      });

      console.log('[Valuation] Emails sent successfully', { email });
      return true;
    } catch (err) {
      console.error('[Valuation] Failed to send email', {
        email,
        error: err instanceof Error ? err.message : String(err),
      });
      return false;
    }
  }

  /**
   * Store valuation request as a lead
   */
  private async storeLead(
    email: string,
    name: string | undefined,
    condoName: string,
    unitNumber: string,
    valuation: ValuationResult | null,
  ): Promise<void> {
    const { error } = await this.supabase.from('leads').insert({
      email,
      name: name ?? null,
      utm_source: 'valuation_tool',
      entry_page: '/buyer-advisory/valuation',
      context: {
        type: 'valuation',
        condo_name: condoName,
        unit_number: unitNumber,
        valuation_result: valuation,
      },
    });

    if (error) {
      console.error('[Valuation] Failed to store lead', {
        email,
        error: error.message,
      });
    }
  }

  /**
   * Main entry point for valuation request
   */
  async requestValuation(req: ValuationRequest): Promise<ValuationResponse> {
    const email = req.email?.trim().toLowerCase();
    const condoName = req.condo_name?.trim();
    const unitNumber = req.unit_number?.trim();
    const sqft = req.sqft;

    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      return { ok: false, error: 'Valid email is required.' };
    }
    if (!condoName) {
      return { ok: false, error: 'Condo name is required.' };
    }
    if (!unitNumber) {
      return { ok: false, error: 'Unit number is required.' };
    }
    if (!sqft || sqft <= 0) {
      return { ok: false, error: 'Unit size (sqft) is required.' };
    }

    console.log('[Valuation] Request received', { email, condoName, unitNumber, sqft });

    // Calculate valuation
    const valuation = await this.calculateValuation(condoName, unitNumber, sqft);

    // Store lead regardless of valuation result
    await this.storeLead(email, req.name, condoName, unitNumber, valuation);

    if (!valuation) {
      // Still send a "we'll review" email if no data available
      console.log('[Valuation] No data available, sending manual review notice', {
        email,
        condoName,
      });
      return {
        ok: true,
        message:
          'Thank you! We have limited transaction data for this property. Michael will personally review and send you a valuation within 24 hours.',
      };
    }

    // Send emails in background (don't block response)
    this.sendValuationEmail(email, req.name, valuation).catch((err) => {
      console.error('[Valuation] Background email failed', {
        email,
        error: err instanceof Error ? err.message : String(err),
      });
    });

    return {
      ok: true,
      message:
        'Thank you! Your property valuation report will be sent to your email shortly.',
    };
  }

  /**
   * Get list of condo names for autocomplete
   */
  async getCondoNames(): Promise<string[]> {
    const { data } = await this.supabase
      .from('condo_sale_transactions')
      .select('condo_name')
      .not('condo_name', 'is', null)
      .not('sale_price', 'is', null);

    if (!data) return [];

    // Get unique names, sorted
    const names = [...new Set(data.map((r) => r.condo_name as string))];
    names.sort((a, b) => a.localeCompare(b));
    return names;
  }

  /**
   * Parse unit number to extract floor and unit separately
   * e.g. "12-05" -> { floor: 12, unit: "05" }
   * e.g. "#03-08" -> { floor: 3, unit: "08" }
   */
  private parseUnitNumber(unitNumber: string): { floor: number; unit: string } | null {
    const cleaned = unitNumber.replace(/^#/, '').trim();

    // Format: "12-05" or "12 - 05"
    const dashMatch = cleaned.match(/^(\d+)\s*-\s*(\d+)$/);
    if (dashMatch) {
      return {
        floor: parseInt(dashMatch[1], 10),
        unit: dashMatch[2],
      };
    }

    // Format: "1205" (4 digits, first 2 are floor, last 2 are unit)
    if (/^\d{4}$/.test(cleaned)) {
      return {
        floor: parseInt(cleaned.substring(0, 2), 10),
        unit: cleaned.substring(2),
      };
    }

    return null;
  }

  /**
   * Get exact unit sqft from DB if we have that specific unit
   * Returns null if not found
   */
  async getExactUnitSqft(
    condoName: string,
    unitNumber: string,
  ): Promise<number | null> {
    const parsed = this.parseUnitNumber(unitNumber);
    if (!parsed) return null;

    // Try to find exact match with exact_level and exact_unit
    const { data: exactMatch } = await this.supabase
      .from('condo_sale_transactions')
      .select('sqft')
      .ilike('condo_name', condoName)
      .eq('exact_level', parsed.floor)
      .eq('exact_unit', parsed.unit)
      .not('sqft', 'is', null)
      .order('sale_date', { ascending: false })
      .limit(1);

    if (exactMatch && exactMatch.length > 0 && exactMatch[0].sqft) {
      return exactMatch[0].sqft as number;
    }

    // Also try matching just the floor within level_low/level_high range
    // and sqft for that floor range (if only one sqft exists for that floor)
    const { data: floorMatch } = await this.supabase
      .from('condo_sale_transactions')
      .select('sqft')
      .ilike('condo_name', condoName)
      .lte('level_low', parsed.floor)
      .gte('level_high', parsed.floor)
      .not('sqft', 'is', null);

    if (floorMatch && floorMatch.length > 0) {
      // Get unique sqft values for this floor
      const sqftSet = new Set(floorMatch.map((t) => t.sqft as number));
      // Only return if there's exactly one sqft value (unambiguous)
      if (sqftSet.size === 1) {
        return [...sqftSet][0];
      }
    }

    return null;
  }
}
