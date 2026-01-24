import { Inject, Injectable } from '@nestjs/common';
import type { SupabaseClient } from '@supabase/supabase-js';
import { SUPABASE_CLIENT } from '../supabase/supabase.constants';

export type MarketTrend = {
  condo_name: string;
  current_avg_psf: number;
  previous_avg_psf: number | null;
  percent_change: number | null;
  period_label: string; // e.g. "Last 30 days" or "Latest period"
};

@Injectable()
export class MarketService {
  constructor(
    @Inject(SUPABASE_CLIENT) private readonly supabase: SupabaseClient,
  ) {}

  async getCondoTrend(condoName: string): Promise<MarketTrend | null> {
    const today = new Date();
    const thirtyDaysInMs = 30 * 24 * 60 * 60 * 1000;

    // Fetch all transactions for this condo, sorted by date descending
    const { data: txns, error } = await this.supabase
      .from('sale_transactions')
      .select('sale_price, sqft, sale_date')
      .ilike('condo_name', condoName)
      .not('sale_price', 'is', null)
      .not('sqft', 'is', null)
      .order('sale_date', { ascending: false });

    if (error || !txns || txns.length === 0) {
      return null;
    }

    // Group transactions into 30-day buckets starting from today
    // Bucket 0: [today - 30d, today]
    // Bucket 1: [today - 60d, today - 30d]
    // ...
    const buckets: { psfs: number[] }[] = [];
    
    for (const t of txns) {
      const saleDate = new Date(t.sale_date as string);
      const diffMs = today.getTime() - saleDate.getTime();
      const bucketIdx = Math.floor(diffMs / thirtyDaysInMs);
      
      if (bucketIdx < 0) continue; // Future date? Skip.
      
      if (!buckets[bucketIdx]) {
        buckets[bucketIdx] = { psfs: [] };
      }
      buckets[bucketIdx].psfs.push((t.sale_price as number) / (t.sqft as number));
    }

    // Find the first bucket with data
    let currentBucketIdx = -1;
    for (let i = 0; i < buckets.length; i++) {
      if (buckets[i] && buckets[i].psfs.length > 0) {
        currentBucketIdx = i;
        break;
      }
    }

    if (currentBucketIdx === -1) return null;

    // Find the previous bucket with data (older than currentBucketIdx)
    let previousBucketIdx = -1;
    for (let i = currentBucketIdx + 1; i < buckets.length; i++) {
      if (buckets[i] && buckets[i].psfs.length > 0) {
        previousBucketIdx = i;
        break;
      }
    }

    const currentAvg = this.avg(buckets[currentBucketIdx].psfs);
    const previousAvg = previousBucketIdx !== -1 ? this.avg(buckets[previousBucketIdx].psfs) : null;
    
    let percentChange: number | null = null;
    if (previousAvg !== null && previousAvg > 0) {
      percentChange = ((currentAvg - previousAvg) / previousAvg) * 100;
      // Round to 1 decimal place
      percentChange = Math.round(percentChange * 10) / 10;
    }

    return {
      condo_name: condoName,
      current_avg_psf: Math.round(currentAvg),
      previous_avg_psf: previousAvg ? Math.round(previousAvg) : null,
      percent_change: percentChange,
      period_label: currentBucketIdx === 0 ? 'Last 30 days' : 'Latest period',
    };
  }

  private avg(nums: number[]): number {
    if (nums.length === 0) return 0;
    return nums.reduce((a, b) => a + b, 0) / nums.length;
  }
}
