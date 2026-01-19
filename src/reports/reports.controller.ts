import { Body, Controller, Get, Post, Query } from '@nestjs/common';
import { ReportsService } from './reports.service';
import type {
  CondoSaleProfitabilityByYearResponse,
  CondoSaleProfitabilityResponse,
  CondoSaleProfitabilityRowsResponse,
  SendMarketOutlook2026Request,
} from './reports.types';

@Controller('reports')
export class ReportsController {
  constructor(private readonly service: ReportsService) {}

  @Get('condo-sale-profitability')
  condoSaleProfitability(
    @Query('condo') condo?: string,
  ): Promise<CondoSaleProfitabilityResponse> {
    return this.service.getCondoSaleProfitability(condo);
  }

  @Get('condo-sale-profitability-by-year')
  condoSaleProfitabilityByYear(
    @Query('condos') condos?: string,
  ): Promise<CondoSaleProfitabilityByYearResponse> {
    // NOTE: Typescript-eslint sometimes marks injected services as `error` typed during
    // incremental program analysis. Narrow the callable surface here to keep linting safe.
    const svc = this.service as ReportsService & {
      getCondoSaleProfitabilityByYear: (
        condosCsv?: string,
      ) => Promise<CondoSaleProfitabilityByYearResponse>;
    };
    return svc.getCondoSaleProfitabilityByYear(condos);
  }

  @Get('condo-sale-profitability-rows')
  condoSaleProfitabilityRows(
    @Query('condos') condos?: string,
  ): Promise<CondoSaleProfitabilityRowsResponse> {
    const svc = this.service as ReportsService & {
      getCondoSaleProfitabilityRows: (
        condosCsv?: string,
      ) => Promise<CondoSaleProfitabilityRowsResponse>;
    };
    return svc.getCondoSaleProfitabilityRows(condos);
  }

  @Post('market-outlook-2026')
  sendMarketOutlook(@Body() body: SendMarketOutlook2026Request) {
    return this.service.sendMarketOutlook2026(body);
  }
}
