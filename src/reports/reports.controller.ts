import { Body, Controller, Post } from '@nestjs/common';
import { ReportsService } from './reports.service';
import type { SendMarketOutlook2026Request } from './reports.types';

@Controller('reports')
export class ReportsController {
  constructor(private readonly service: ReportsService) {}

  @Post('market-outlook-2026')
  async sendMarketOutlook(@Body() body: SendMarketOutlook2026Request) {
    return this.service.sendMarketOutlook2026(body);
  }
}


