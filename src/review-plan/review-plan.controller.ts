import { Body, Controller, Post } from '@nestjs/common';
import { ReviewPlanService } from './review-plan.service';
import type { ReviewPlanRequest } from './review-plan.types';

@Controller('review-plan')
export class ReviewPlanController {
  constructor(private readonly service: ReviewPlanService) {}

  @Post()
  async send(@Body() body: ReviewPlanRequest) {
    if (!body?.email || !body?.selections) {
      return { ok: false, error: 'Missing email or selections.' };
    }
    return this.service.sendPlan(body);
  }
}


