import { Inject, Injectable } from '@nestjs/common';
import type { SupabaseClient } from '@supabase/supabase-js';
import { SUPABASE_CLIENT } from '../supabase/supabase.constants';
import type { ListingRow } from './listings.types';

@Injectable()
export class ListingsService {
  constructor(@Inject(SUPABASE_CLIENT) private readonly supabase: SupabaseClient) {}

  async list(): Promise<ListingRow[]> {
    const { data, error } = await this.supabase
      .from('listings')
      .select('*')
      .order('listed_at', { ascending: false, nullsFirst: false });

    if (error) throw new Error(error.message);
    return (data ?? []) as ListingRow[];
  }
}


