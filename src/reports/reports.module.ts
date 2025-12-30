import { Module } from '@nestjs/common';
import { LeadsModule } from '../leads/leads.module';
import { ReportsController } from './reports.controller';
import { ReportsService } from './reports.service';

@Module({
  imports: [LeadsModule],
  controllers: [ReportsController],
  providers: [ReportsService],
})
export class ReportsModule {}


