import { Module } from '@nestjs/common';
import { ReviewPlanController } from './review-plan.controller';
import { ReviewPlanService } from './review-plan.service';

@Module({
  controllers: [ReviewPlanController],
  providers: [ReviewPlanService],
})
export class ReviewPlanModule {}


