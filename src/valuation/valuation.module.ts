import { Module } from '@nestjs/common';
import { SupabaseModule } from '../supabase/supabase.module';
import { ValuationController } from './valuation.controller';
import { ValuationService } from './valuation.service';

@Module({
  imports: [SupabaseModule],
  controllers: [ValuationController],
  providers: [ValuationService],
})
export class ValuationModule {}
