import { Inject, Injectable } from '@nestjs/common';
import type { SupabaseClient } from '@supabase/supabase-js';
import { SUPABASE_CLIENT } from '../supabase/supabase.constants';
import type { CreateLeadRequest } from './leads.types';

@Injectable()
export class LeadsService {
  constructor(@Inject(SUPABASE_CLIENT) private readonly supabase: SupabaseClient) {}

  async createLead(body: CreateLeadRequest) {
    const name = body.name?.trim() || null;
    const email = body.email?.trim().toLowerCase() || null;
    const phone = body.phone?.trim() || null;

    const utm = body.utm ?? {};
    const context = body.context ?? {};

    const firstSeen =
      typeof context.first_seen_at === 'string' ? context.first_seen_at : null;

    const { error, data } = await this.supabase
      .from('leads')
      .insert({
        name,
        email,
        phone,
        utm_source: utm.utm_source ?? null,
        utm_medium: utm.utm_medium ?? null,
        utm_campaign: utm.utm_campaign ?? null,
        utm_term: utm.utm_term ?? null,
        utm_content: utm.utm_content ?? null,
        entry_page: context.entry_page ?? null,
        referrer: context.referrer ?? null,
        first_seen_at: firstSeen,
      })
      .select('id')
      .maybeSingle();

    if (error) {
      console.log('[Leads] Failed to insert lead', {
        email,
        message: error.message,
      });
      return { ok: false as const, error: 'Failed to store lead.' };
    }

    return { ok: true as const, id: data?.id };
  }
}


