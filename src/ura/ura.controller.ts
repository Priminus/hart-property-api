import { Controller, Post, Headers } from '@nestjs/common';
import { UraService } from './ura.service';

@Controller('ura')
export class UraController {
  constructor(private readonly uraService: UraService) {}

  /**
   * POST /ura/ingest
   * Manually trigger URA transaction ingestion
   * Requires admin authentication
   */
  @Post('ingest')
  async triggerIngestion(
    @Headers('authorization') authHeader?: string,
  ): Promise<{
    ok: boolean;
    total?: number;
    condos?: number;
    inserted?: number;
    error?: string;
  }> {
    // Simple auth check - require some form of authorization
    const token = (authHeader ?? '').replace(/^Bearer\s+/i, '').trim();
    if (!token) {
      return { ok: false, error: 'Unauthorized' };
    }

    // Verify token with Supabase
    const url = process.env.SUPABASE_URL;
    const apiKey = process.env.SUPABASE_API_KEY;
    if (!url || !apiKey) {
      return { ok: false, error: 'Server misconfigured' };
    }

    try {
      const res = await fetch(`${url.replace(/\/$/, '')}/auth/v1/user`, {
        headers: { apikey: apiKey, authorization: `Bearer ${token}` },
      });
      if (!res.ok) {
        return { ok: false, error: 'Unauthorized' };
      }
    } catch {
      return { ok: false, error: 'Auth check failed' };
    }

    try {
      const result = await this.uraService.ingestTransactions();
      return { ok: true, ...result };
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Unknown error';
      return { ok: false, error: message };
    }
  }
}
