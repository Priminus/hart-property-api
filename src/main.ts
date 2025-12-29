import 'dotenv/config';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors({
    origin: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(',') : true,
    credentials: true,
  });
  const port = Number(process.env.PORT ?? 3001);
  await app.listen(port);

  // Always print effective port for visibility in logs (local + prod).
  const url = await app.getUrl();
  console.log(`[API] Listening on ${url} (port=${port})`);

  // Helpful hint when running in serverless-style environments where "listening" may not apply.
  if (process.env.VERCEL) {
    console.log(
      '[API] Detected Vercel environment; requests may run as Serverless Functions rather than a long-lived server.',
    );
  }
}
bootstrap();
