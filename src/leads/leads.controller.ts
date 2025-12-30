import { Body, Controller, Post } from '@nestjs/common';
import { LeadsService } from './leads.service';
import type { CreateLeadRequest } from './leads.types';

@Controller('leads')
export class LeadsController {
  constructor(private readonly service: LeadsService) {}

  @Post()
  async create(@Body() body: CreateLeadRequest) {
    // Keep validation minimal; we don't want to drop leads on strict validation.
    return this.service.createLead(body ?? {});
  }
}


