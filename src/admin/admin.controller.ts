import {
  Body,
  Controller,
  Get,
  Headers,
  Post,
  Query,
  UnauthorizedException,
} from '@nestjs/common';
import { AdminService } from './admin.service';
import type {
  ListCondoNamesResponse,
  ListSaleTransactionsResponse,
  ListListingsResponse,
  UpsertSaleTransactionRequest,
  UpsertSaleTransactionResponse,
  UpsertListingRequest,
  UpsertListingResponse,
} from './admin.types';

@Controller('admin')
export class AdminController {
  constructor(private readonly service: AdminService) {}

  @Get('property-transactions/condos')
  async condos(
    @Headers('authorization') authorization: string | undefined,
  ): Promise<ListCondoNamesResponse> {
    const email = await this.service.requireLoggedInUserEmail(authorization);
    if (!email) throw new UnauthorizedException('Unauthorized.');
    return this.service.listCondoNames();
  }

  @Get('property-transactions')
  async list(
    @Headers('authorization') authorization: string | undefined,
    @Query('condo') condo?: string,
    @Query('limit') limit?: string,
    @Query('offset') offset?: string,
  ): Promise<ListSaleTransactionsResponse> {
    const email = await this.service.requireLoggedInUserEmail(authorization);
    if (!email) throw new UnauthorizedException('Unauthorized.');
    return this.service.listSaleTransactions({
      condo,
      limit: limit ? Number(limit) : undefined,
      offset: offset ? Number(offset) : undefined,
    });
  }

  @Post('property-transactions')
  async upsert(
    @Headers('authorization') authorization: string | undefined,
    @Body() body: UpsertSaleTransactionRequest,
  ): Promise<UpsertSaleTransactionResponse> {
    const email = await this.service.requireLoggedInUserEmail(authorization);
    if (!email) throw new UnauthorizedException('Unauthorized.');
    return this.service.upsertSaleTransaction(body);
  }

  @Get('listings')
  async listings(
    @Headers('authorization') authorization: string | undefined,
    @Query('limit') limit?: string,
    @Query('offset') offset?: string,
  ): Promise<ListListingsResponse> {
    const email = await this.service.requireLoggedInUserEmail(authorization);
    if (!email) throw new UnauthorizedException('Unauthorized.');
    return this.service.listListings({
      limit: limit ? Number(limit) : undefined,
      offset: offset ? Number(offset) : undefined,
    });
  }

  @Post('listings')
  async upsertListing(
    @Headers('authorization') authorization: string | undefined,
    @Body() body: UpsertListingRequest,
  ): Promise<UpsertListingResponse> {
    const email = await this.service.requireLoggedInUserEmail(authorization);
    if (!email) throw new UnauthorizedException('Unauthorized.');
    return this.service.upsertListing(body);
  }
}
