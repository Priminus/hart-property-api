import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ArticlesModule } from './articles/articles.module';
import { ListingsModule } from './listings/listings.module';
import { ReviewPlanModule } from './review-plan/review-plan.module';
import { SupabaseModule } from './supabase/supabase.module';
import { FaviconController } from './favicon.controller';

@Module({
  imports: [
    SupabaseModule,
    ArticlesModule,
    ListingsModule,
    ReviewPlanModule,
  ],
  controllers: [AppController, FaviconController],
  providers: [AppService],
})
export class AppModule {}
