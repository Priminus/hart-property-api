import { Body, Controller, Get, Post, Query } from '@nestjs/common';
import { ValuationService } from './valuation.service';
import type { ValuationRequest, ValuationResponse } from './valuation.types';

@Controller('valuation')
export class ValuationController {
  constructor(private readonly service: ValuationService) {}

  @Post()
  async requestValuation(
    @Body() body: ValuationRequest,
  ): Promise<ValuationResponse> {
    return this.service.requestValuation(body);
  }

  @Get('condos')
  async getCondoNames(): Promise<{ ok: true; condos: string[] }> {
    const condos = await this.service.getCondoNames();
    return { ok: true, condos };
  }

  /**
   * Look up exact unit sqft from DB based on condo name and unit number
   * Returns sqft only if exact match found
   */
  @Get('unit-info')
  async getUnitInfo(
    @Query('condo') condo: string,
    @Query('unit') unit: string,
  ): Promise<{ ok: boolean; sqft?: number; error?: string }> {
    if (!condo || !unit) {
      return { ok: false, error: 'Condo name and unit number required' };
    }
    const sqft = await this.service.getExactUnitSqft(condo, unit);
    return { ok: true, sqft: sqft ?? undefined };
  }
}
