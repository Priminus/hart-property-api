import { Controller, Get, Res } from '@nestjs/common';
import type { Response } from 'express';

// Browsers may request /favicon.ico and /favicon.png from any origin they talk to (including the API),
// especially if the API base URL is opened directly or used as the page origin in some environments.
// Handle these explicitly to avoid noisy 404 logs.
@Controller()
export class FaviconController {
  @Get(['/favicon.ico', '/favicon.png'])
  favicon(@Res() res: Response) {
    // No favicon served from the API.
    return res.status(204).end();
  }
}


