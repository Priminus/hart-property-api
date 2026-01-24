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
  async searchCondoNames(
    @Query('q') query: string,
  ): Promise<{ ok: true; condos: string[] }> {
    const condos = await this.service.searchCondoNames(query || '');
    return { ok: true, condos };
  }

  /**
   * Look up exact unit sqft from DB based on condo name and floor/unit
   * Returns sqft only if exact match found
   */
  @Get('unit-info')
  async getUnitInfo(
    @Query('condo') condo: string,
    @Query('floor') floor: string,
    @Query('unit') unit: string,
  ): Promise<{ ok: boolean; sqft?: number; error?: string }> {
    if (!condo) {
      return { ok: false, error: 'Condo name required' };
    }
    const floorNum = parseInt(floor, 10);
    if (isNaN(floorNum) || !unit) {
      return { ok: true }; // No error, just no sqft lookup possible
    }
    const sqft = await this.service.getExactUnitSqftByFloorUnit(
      condo,
      floorNum,
      unit,
    );
    return { ok: true, sqft: sqft ?? undefined };
  }

  /**
   * Check if a property has floor data (is a condo vs landed)
   */
  @Get('property-type')
  async getPropertyType(
    @Query('condo') condo: string,
  ): Promise<{ ok: boolean; hasFloorData: boolean; error?: string }> {
    if (!condo) {
      return { ok: false, hasFloorData: false, error: 'Condo name required' };
    }
    const hasFloorData = await this.service.hasFloorData(condo);
    return { ok: true, hasFloorData };
  }
}
