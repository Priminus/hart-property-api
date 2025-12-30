import fs from 'node:fs/promises';
import path from 'node:path';
import { Injectable } from '@nestjs/common';
import { validate as validateEmail } from '@mailtester/core';
import * as postmark from 'postmark';
import { LeadsService } from '../leads/leads.service';
import type { SendMarketOutlook2026Request } from './reports.types';

@Injectable()
export class ReportsService {
  constructor(private readonly leads: LeadsService) {}

  sendMarketOutlook2026(body: SendMarketOutlook2026Request) {
    const email = (body.email ?? '').trim().toLowerCase();
    if (!email) return { ok: false as const, error: 'Email is required.' };

    const postmarkKey = process.env.POSTMARK_API_KEY;
    const from = process.env.POSTMARK_FROM;
    if (!postmarkKey || !from) {
      console.log('[Reports] Missing Postmark env vars', {
        hasPostmarkKey: Boolean(postmarkKey),
        hasFrom: Boolean(from),
      });
      return { ok: false as const, error: 'Email service not configured.' };
    }

    // Minimal synchronous validation only (background job does deeper validation).
    // This endpoint should return quickly so the UI can show "Sent!" immediately.
    const basicEmailOk = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
    if (!basicEmailOk) {
      return { ok: false as const, error: 'Invalid email.' };
    }

    // Fire-and-forget background work (do not await).
    this.runBackgroundSendMarketOutlook2026({
      email,
      utm: body.utm,
      context: body.context,
    }).catch((err) => {
      const e = err as { message?: unknown };
      console.log('[Reports] Background send failed', {
        email,
        message: typeof e?.message === 'string' ? e.message : String(err),
      });
    });

    console.log('[Reports] Accepted report request (background send started)', {
      email,
    });

    return { ok: true as const, message: 'Sending report.' };
  }

  private async runBackgroundSendMarketOutlook2026(
    body: SendMarketOutlook2026Request,
  ) {
    const email = (body.email ?? '').trim().toLowerCase();

    const postmarkKey = process.env.POSTMARK_API_KEY;
    const from = process.env.POSTMARK_FROM;
    if (!postmarkKey || !from) {
      throw new Error('Missing POSTMARK_API_KEY or POSTMARK_FROM');
    }

    console.log('[Reports] Background send entered', { email });

    // Deeper email validation (still bounded).
    const validation = await validateEmail(email, {
      preset: 'balanced',
      earlyExit: true,
      timeout: 3500,
      validators: { smtp: { enabled: false } },
    });
    const reason = validation.reason ?? 'invalid';
    if (!validation.valid && reason !== 'disposable') {
      console.log('[Reports] Email validation failed', { email, reason });
      return;
    }

    // Lead capture (store attribution/context).
    await this.leads.createLead({
      email,
      utm: body.utm,
      context: body.context,
    });

    const pdfPath = path.resolve(
      process.cwd(),
      'content/assets/marketoutlook2026.pdf',
    );
    const pdf = await fs.readFile(pdfPath);

    const subject = 'Singapore Market Outlook 2026 (Report)';
    const logoUrl = process.env.PUBLIC_ASSETS_BASE_URL
      ? `${process.env.PUBLIC_ASSETS_BASE_URL.replace(/\/$/, '')}/hart-logo.png`
      : 'https://hartproperty.sg/hart-logo.png';

    const textBody =
      `Here is the Singapore Market Outlook 2026 report (PDF attached).\n\n` +
      `You received this because you requested it on hartproperty.sg.\n` +
      `If this wasn’t you, reply to this email.\n`;

    const htmlBody = `
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width" />
    <title>${subject}</title>
  </head>
  <body style="margin:0;padding:0;background:#F7F6F3;">
    <div style="max-width:640px;margin:0 auto;padding:24px;">
      <div style="background:#ffffff;border:1px solid rgba(62,92,138,0.15);border-radius:14px;overflow:hidden;">
        <div style="padding:22px 22px 14px 22px;border-bottom:1px solid rgba(62,92,138,0.15);">
          <div style="display:flex;align-items:center;">
            <img src="${logoUrl}" alt="Hart Property" width="44" height="44" style="display:block;margin-right:16px;" />
            <div style="font-family:Inter,Arial,sans-serif;">
              <div style="font-size:16px;font-weight:800;letter-spacing:-0.02em;color:#13305D;">HART PROPERTY</div>
              <div style="font-size:12px;color:#3E5C8A;">Singapore Market Outlook 2026</div>
            </div>
          </div>
        </div>

        <div style="padding:22px;font-family:Inter,Arial,sans-serif;color:#1F2933;line-height:1.55;">
          <div style="font-size:16px;font-weight:700;color:#13305D;margin-bottom:10px;">Your PDF report is attached</div>
          <div style="font-size:13px;color:#3E5C8A;">
            You received this because you requested it on
            <a href="https://hartproperty.sg" style="color:#4C7DBF;text-decoration:none;">hartproperty.sg</a>.
            If this wasn’t you, please reply to this email.
          </div>
        </div>

        <div style="padding:16px 22px;border-top:1px solid rgba(62,92,138,0.15);font-family:JetBrains Mono,ui-monospace,Menlo,Monaco,Consolas,monospace;font-size:11px;line-height:1.6;color:#3E5C8A;">
          <div>HART PROPERTY • Michael Hart | CEA Registration: R071893C | Agency License: L3008022J</div>
        </div>
      </div>
    </div>
  </body>
</html>
`.trim();

    const client = new postmark.ServerClient(postmarkKey);
    await client.sendEmail({
      MessageStream: 'outbound',
      From: `Hart Property <${from}>`,
      To: email,
      Bcc: 'michael.hart@hartproperty.sg',
      ReplyTo: `Hart Property <${from}>`,
      Subject: subject,
      TextBody: textBody,
      HtmlBody: htmlBody,
      Attachments: [
        {
          Name: 'Singapore-Market-Outlook-2026.pdf',
          Content: pdf.toString('base64'),
          ContentType: 'application/pdf',
          ContentID: null,
        },
      ],
    });

    console.log('[Reports] Background send completed', { email });
  }
}
