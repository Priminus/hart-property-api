import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ArticlesModule } from './articles/articles.module';
import { ListingsModule } from './listings/listings.module';
import { ReviewPlanModule } from './review-plan/review-plan.module';
import { SupabaseModule } from './supabase/supabase.module';
import { FaviconController } from './favicon.controller';
import { LeadsModule } from './leads/leads.module';
import { ReportsModule } from './reports/reports.module';
import { AdminModule } from './admin/admin.module';
import { UraModule } from './ura/ura.module';
import { ValuationModule } from './valuation/valuation.module';

@Module({
  imports: [
    ScheduleModule.forRoot(),
    SupabaseModule,
    ArticlesModule,
    ListingsModule,
    ReviewPlanModule,
    LeadsModule,
    ReportsModule,
    AdminModule,
    UraModule,
    ValuationModule,
  ],
  controllers: [AppController, FaviconController],
  providers: [AppService],
})
export class AppModule {}
