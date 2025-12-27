import { Global, Module } from '@nestjs/common';
import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import { SUPABASE_CLIENT } from './supabase.constants';

@Global()
@Module({
  providers: [
    {
      provide: SUPABASE_CLIENT,
      useFactory: (): SupabaseClient => {
        const url = process.env.SUPABASE_URL;
        const apiKey = process.env.SUPABASE_API_KEY;

        if (!url) throw new Error('Missing SUPABASE_URL');
        if (!apiKey) throw new Error('Missing SUPABASE_API_KEY');

        const typedCreateClient = createClient as unknown as (
          supabaseUrl: string,
          supabaseKey: string,
          options?: Parameters<typeof createClient>[2],
        ) => SupabaseClient;

        return typedCreateClient(url, apiKey, {
          auth: { persistSession: false },
        });
      },
    },
  ],
  exports: [SUPABASE_CLIENT],
})
export class SupabaseModule {}
