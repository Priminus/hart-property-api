import { Module } from '@nestjs/common';
import { UraService } from './ura.service';
import { UraController } from './ura.controller';
import { SupabaseModule } from '../supabase/supabase.module';

@Module({
  imports: [SupabaseModule],
  providers: [UraService],
  controllers: [UraController],
  exports: [UraService],
})
export class UraModule {}
