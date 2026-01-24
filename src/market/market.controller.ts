import { Controller, Get, Query, BadRequestException } from '@nestjs/common';
import { MarketService, MarketTrend } from './market.service';

@Controller('market')
export class MarketController {
  constructor(private readonly marketService: MarketService) {}

  @Get('trend')
  async getTrend(@Query('condo') condoName: string): Promise<MarketTrend> {
    if (!condoName) {
      throw new BadRequestException('Condo name is required');
    }
    const trend = await this.marketService.getCondoTrend(condoName);
    if (!trend) {
      // Return a neutral trend if no data found
      return {
        condo_name: condoName,
        current_avg_psf: 0,
        previous_avg_psf: null,
        percent_change: 0,
        period_label: 'No data',
      };
    }
    return trend;
  }

  // Batch endpoint for the home page
  @Get('trends')
  async getTrends(@Query('condos') condosStr: string): Promise<MarketTrend[]> {
    const condos = condosStr ? condosStr.split(',') : [];
    if (condos.length === 0) {
      return [];
    }

    const results = await Promise.all(
      condos.map(name => this.marketService.getCondoTrend(name))
    );

    // Filter out nulls and fill with defaults if needed
    return results.map((trend, i) => {
      if (trend) return trend;
      return {
        condo_name: condos[i],
        current_avg_psf: 0,
        previous_avg_psf: null,
        percent_change: 0,
        period_label: 'No data',
      };
    });
  }
}
