import fs from 'node:fs/promises';
import path from 'node:path';
import PDFDocument from 'pdfkit';
import SVGtoPDF from 'svg-to-pdfkit';
import { Inject, Injectable } from '@nestjs/common';
import { validate as validateEmail } from '@mailtester/core';
import * as postmark from 'postmark';
import OpenAI from 'openai';
import cloudscraper from 'cloudscraper';
import type { SupabaseClient } from '@supabase/supabase-js';
import { SUPABASE_CLIENT } from '../supabase/supabase.constants';
import type {
  ReviewPlanRequest,
  ReviewPlanSelections,
} from './review-plan.types';

/* eslint-disable
  @typescript-eslint/no-unsafe-assignment,
  @typescript-eslint/no-unsafe-call,
  @typescript-eslint/no-unsafe-member-access
*/

type AssessResult = { status: 'pass' | 'fail' | 'incomplete'; message: string };

type GptListing = {
  condo: string;
  bedrooms: number[];
  price_lower: string;
  price_higher: string;
  size_lower: string;
  size_higher: string;
  listings: Array<{
    bedrooms: number;
    price: string;
    size: string;
    url: string;
  }>;
};

type CashCpf = { cash: number; cpf: number };
type CostBreakdown = {
  price: number;
  downpaymentTotal: number;
  loanTenure: number;
  loanRatio: number;
  maxLoan: number;
  minCashDown: number;
  bsd: number;
  bsdRate: number;
  legalFees: number;
  miscFees: number;
  totalUpfront: number;
  cpfUsed: number;
  cashReqAfterCPF: number;
  cpfRemaining: number;
  cashRemaining: number;
  cashShortfall: number; // >0 means insufficient cash
};

const STRESS_RATE = 0.045 / 12; // 4.5% annual, monthly
const REF_TENURE_MONTHS = 30 * 12;

function calculateLoanRatio(months: number): number {
  if (months >= REF_TENURE_MONTHS) return 1;
  const num = 1 - Math.pow(1 + STRESS_RATE, -months);
  const den = 1 - Math.pow(1 + STRESS_RATE, -REF_TENURE_MONTHS);
  return num / den;
}

function getAgeFromRange(ageRange: string): number {
  switch (ageRange) {
    case 'age1':
      return 25; // <30
    case 'age2':
      return 32; // 30-34
    case 'age3':
      return 37; // 35-39
    case 'age4':
      return 45; // 40+
    default:
      return 35;
  }
}

const labelMaps = {
  buyerType: {
    single: 'Single',
    couple: 'Couple',
    mixed: 'Mixed-nationality household',
  },
  ageRange: { age1: '<30', age2: '30–34', age3: '35–39', age4: '40+' },
  yesNo: { yes: 'Yes', no: 'No', unsure: 'Not sure' },
  cashRange: { c1: '<$150k', c2: '$150–250k', c3: '$250–400k', c4: '$400k+' },
  cpfRange: { cpf1: '<$50k', cpf2: '$50–150k', cpf3: '$150k+' },
  bufferRange: { b1: '<3', b2: '3–6', b3: '6–12', b4: '12+' },
  priceRange: {
    p1: '$0.9–1.2m',
    p2: '$1.2–1.6m',
    p3: '$1.6–2.0m',
    p4: '$2.0m+',
  },
  locationPref: { ocr: 'OCR', rcr: 'RCR', ccr: 'CCR', flexible: 'Flexible' },
  launchType: { new: 'New', resale: 'Resale', open: 'Open' },
  holdingPeriod: { h1: '<3 yrs', h2: '3–5 yrs', h3: '5–10 yrs', h4: '10+ yrs' },
  exitOption: {
    upgrade: 'Sell to upgrade',
    hold: 'Hold long-term',
    rent: 'Rent out',
    unsure: 'Unsure',
  },
  targetBuyer: {
    investor: 'Investor',
    hdb: 'HDB upgrader',
    owner: 'Another owner-occupier',
    unsure: 'Unsure',
  },
  stability: { vstable: 'Very stable', stable: 'Stable', variable: 'Variable' },
  lifeChanges: { career: 'Career switch', kids: 'Kids', none: 'None' },
} as const;

function mapLabel(map: Record<string, string>, key: string) {
  return map[key] ?? key ?? '';
}

function money(n: number) {
  const rounded = Math.round(n);
  return `S$${rounded.toLocaleString('en-SG')}`;
}

type SoraSnapshot = {
  oneMonth: number; // percent, e.g. 1.16900
  sixMonth: number; // percent, e.g. 1.32760
  asAt?: string; // dd/mm/yyyy
};

let soraCache: { fetchedAt: number; data: SoraSnapshot } | null = null;

async function fetchSoraSnapshot(): Promise<SoraSnapshot | null> {
  const cacheTtlMs = 6 * 60 * 60 * 1000; // 6 hours
  const now = Date.now();
  if (soraCache && now - soraCache.fetchedAt < cacheTtlMs) {
    return soraCache.data;
  }

  const url = 'https://housingloansg.com/hl/charts/sibor-sor-daily-chart';
  try {
    const html = (await (cloudscraper as any).get({
      uri: url,
      // Keep a UA set; some hosts behave differently without it.
      headers: {
        'User-Agent':
          'Mozilla/5.0 (compatible; HartPropertyBot/1.0; +https://hartproperty.sg)',
      },
      timeout: 15_000,
    })) as string;

    const oneM = html.match(
      /<td[^>]*>\s*1\s*Mth\s*<\/td>\s*<td[^>]*>\s*([0-9]+(?:\.[0-9]+)?)\s*<\/td>/i,
    )?.[1];
    const sixM = html.match(
      /<td[^>]*>\s*6\s*Mth\s*<\/td>\s*<td[^>]*>\s*([0-9]+(?:\.[0-9]+)?)\s*<\/td>/i,
    )?.[1];
    const asAt = html.match(
      /<td[^>]*>\s*As\s*at\s*<\/td>\s*<td[^>]*>\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4})\s*<\/td>/i,
    )?.[1];

    const oneMonth = oneM ? Number(oneM) : NaN;
    const sixMonth = sixM ? Number(sixM) : NaN;
    if (!Number.isFinite(oneMonth) || !Number.isFinite(sixMonth)) {
      console.log('[SORA] Failed to parse SORA snapshot', {
        url,
        oneM,
        sixM,
        asAt,
      });
      return null;
    }

    const data: SoraSnapshot = { oneMonth, sixMonth, asAt };
    soraCache = { fetchedAt: now, data };
    console.log('[SORA] Fetched snapshot', { ...data, url });
    return data;
  } catch (e) {
    const err = e as {
      name?: unknown;
      message?: unknown;
      code?: unknown;
      statusCode?: unknown;
    };
    console.log('[SORA] Failed to fetch snapshot', {
      url,
      name: typeof err?.name === 'string' ? err.name : undefined,
      code: typeof err?.code === 'string' ? err.code : undefined,
      statusCode:
        typeof err?.statusCode === 'number' ? err.statusCode : undefined,
      message: typeof err?.message === 'string' ? err.message : 'Unknown error',
    });
    return null;
  }
}

function computeMonthlyRepayment({
  principal,
  annualRate,
  months,
}: {
  principal: number;
  annualRate: number; // decimal, e.g. 0.03
  months: number;
}): number {
  if (!Number.isFinite(principal) || principal <= 0) return 0;
  if (!Number.isFinite(months) || months <= 0) return 0;
  if (!Number.isFinite(annualRate) || annualRate <= 0) {
    return principal / months;
  }
  const r = annualRate / 12;
  const pow = Math.pow(1 + r, months);
  return (principal * r * pow) / (pow - 1);
}

function parseLowerBoundFromRangeLabel(label: string): number {
  // Examples: "<$150k", "$150–250k", "$250–400k", "$400k+"
  const cleaned = label.replace(/[,\s]/g, '');
  const m = cleaned.match(/\$([0-9]+(?:\.[0-9]+)?)m/i);
  if (m) return Number(m[1]) * 1_000_000;
  const k = cleaned.match(/\$([0-9]+)k/i);
  if (k) return Number(k[1]) * 1_000;
  const plain = cleaned.match(/\$([0-9]+)/);
  if (plain) return Number(plain[1]);
  return 0;
}

function cashCpfFromSelections(selections: ReviewPlanSelections): CashCpf {
  const cashLabel = mapLabel(
    labelMaps.cashRange as unknown as Record<string, string>,
    selections.cashRange,
  );
  const cpfLabel = mapLabel(
    labelMaps.cpfRange as unknown as Record<string, string>,
    selections.cpfRange,
  );
  return {
    cash: parseLowerBoundFromRangeLabel(cashLabel),
    cpf: parseLowerBoundFromRangeLabel(cpfLabel),
  };
}

function pricePointsForRange(priceRange: string): number[] {
  // As requested:
  // p1 (<$1.2m) -> [1.2m]
  // p2 ($1.2–1.6m) -> [1.2m, 1.6m]
  // p3 ($1.6–2.0m) -> [1.6m, 2.0m]
  // p4 ($2.0m+) -> [2.0m]
  switch (priceRange) {
    case 'p1':
      return [1_200_000];
    case 'p2':
      return [1_200_000, 1_600_000];
    case 'p3':
      return [1_600_000, 2_000_000];
    case 'p4':
      return [2_000_000];
    default:
      return [];
  }
}

function computeBsd(price: number) {
  // Singapore BSD (residential) tier rates (as per 2025 schedule):
  // 1% first S$180,000
  // 2% next  S$180,000
  // 3% next  S$640,000
  // 4% next  S$500,000
  // 5% next  S$1,500,000  (i.e. up to S$3,000,000 total)
  // 6% amount exceeding S$3,000,000
  //
  // NOTE: This is BSD only. It excludes ABSD and any future changes.
  if (!Number.isFinite(price) || price <= 0) return 0;
  const tiers: Array<[number, number]> = [
    [180_000, 0.01],
    [180_000, 0.02],
    [640_000, 0.03],
    [500_000, 0.04],
    [1_500_000, 0.05],
    [Number.POSITIVE_INFINITY, 0.06],
  ];
  let remaining = price;
  let tax = 0;
  for (const [cap, rate] of tiers) {
    const chunk = Math.min(remaining, cap);
    if (chunk <= 0) break;
    tax += chunk * rate;
    remaining -= chunk;
  }
  return tax;
}

function computeUpfront({
  price,
  cash,
  cpf,
  ageRange,
}: {
  price: number;
  cash: number;
  cpf: number;
  ageRange: string;
}): CostBreakdown {
  const age = getAgeFromRange(ageRange);
  const loanTenure = Math.min(30, Math.max(0, 65 - age));
  const loanRatio = calculateLoanRatio(loanTenure * 12);
  const maxLoan = price * 0.75 * loanRatio;

  const downpaymentTotal = price - maxLoan;
  const minCashDown = price * 0.05;
  const bsd = computeBsd(price);
  const bsdRate = (bsd / price) * 100;
  const legalFees = 3_000; // estimate
  const miscFees = 2_000; // valuation/admin/incidentals estimate
  const totalUpfront = downpaymentTotal + bsd + legalFees + miscFees;

  // Allocation rule (to preserve cash where possible):
  // - Use cash to satisfy mandatory 5% cash
  // - Use CPF for remaining upfront (downpayment+BSD+fees) as available
  // - Remaining comes from cash
  const cashMin = Math.min(cash, minCashDown);
  let remaining = totalUpfront - cashMin;
  const cpfUsed = Math.min(cpf, remaining);
  remaining -= cpfUsed;
  const cashReqAfterCPF = cashMin + Math.max(0, remaining);

  const cashRemaining = cash - cashReqAfterCPF;
  const cpfRemaining = cpf - cpfUsed;
  const cashShortfall = Math.max(0, -cashRemaining);

  return {
    price,
    downpaymentTotal,
    loanTenure,
    loanRatio,
    maxLoan,
    minCashDown,
    bsd,
    bsdRate,
    legalFees,
    miscFees,
    totalUpfront,
    cpfUsed,
    cashReqAfterCPF,
    cpfRemaining,
    cashRemaining: Math.max(0, cashRemaining),
    cashShortfall,
  };
}

function assess(
  selections: ReviewPlanSelections,
  funds: CashCpf,
): AssessResult {
  const { priceRange, cashRange, bufferRange } = selections;
  if (!priceRange || !cashRange) {
    return {
      status: 'incomplete',
      message: 'Incomplete: missing price range or cash range.',
    };
  }

  const prices = pricePointsForRange(priceRange);
  // We check if any of the target price points result in a shortfall
  for (const p of prices) {
    const b = computeUpfront({
      price: p,
      cash: funds.cash,
      cpf: funds.cpf,
      ageRange: selections.ageRange,
    });
    if (b.cashShortfall > 0) {
      return {
        status: 'fail',
        message:
          'Your declared capital and buffer are not sufficient for this price range. Revisit capital or target price.',
      };
    }
  }

  const priceIndex = ['p1', 'p2', 'p3', 'p4'].indexOf(priceRange);
  const bufferIndex = ['b1', 'b2', 'b3', 'b4'].indexOf(bufferRange);

  // Buffer check still applies
  const bufferOk = bufferIndex >= 1 || priceIndex <= 1;

  if (bufferOk) {
    return {
      status: 'pass',
      message:
        'Your declared capital and buffer look adequate for this price range.',
    };
  }

  return {
    status: 'fail',
    message:
      'Comfort buffer looks thin for this price range. Increase liquidity runway.',
  };
}

async function renderPdf({
  name,
  email,
  selections,
  gptListings = [],
}: {
  name?: string;
  email: string;
  selections: ReviewPlanSelections;
  gptListings?: GptListing[];
}): Promise<Buffer> {
  const doc = new PDFDocument({
    size: 'A4',
    margins: { top: 80, left: 48, right: 48, bottom: 84 }, // reserve space for header/footer
  });

  const chunks: Buffer[] = [];
  const done = new Promise<Buffer>((resolve, reject) => {
    doc.on('data', (d) => chunks.push(d as Buffer));
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);
  });

  const logoPath = path.resolve(process.cwd(), 'assets/hart-logo.svg');
  const logoSvg = await fs.readFile(logoPath, 'utf8');
  const whatsappSvgPath = path.resolve(process.cwd(), 'assets/whatsapp.svg');
  let whatsappSvg: string | null = null;
  try {
    whatsappSvg = await fs.readFile(whatsappSvgPath, 'utf8');
  } catch {
    whatsappSvg = null;
  }

  const bottomLimit = () => doc.page.height - doc.page.margins.bottom;
  const ensureSpace = (needed: number) => {
    if (doc.y + needed > bottomLimit()) {
      doc.addPage();
    }
  };

  const drawHeader = () => {
    const prevY = doc.y;
    doc.save();

    // White background
    doc.rect(0, 0, doc.page.width, 60).fillColor('#FFFFFF').fill();

    // Text in header - color changed to primary blue (#13305D)
    // Using Courier-Bold to mimic JetBrains Mono from the website
    doc
      .font('Courier-Bold')
      .fontSize(16)
      .fillColor('#13305D')
      .text('HART PROPERTY', 94, 23, { align: 'left' });

    // Logo in header
    doc.save();
    SVGtoPDF(doc as unknown as never, logoSvg, 48, 12, {
      width: 36,
      preserveAspectRatio: 'xMinYMin meet',
    });
    doc.restore();

    // Subtle bottom border to match the website's scaffolding line
    doc
      .moveTo(0, 60)
      .lineTo(doc.page.width, 60)
      .strokeColor('#3E5C8A')
      .strokeOpacity(0.15)
      .lineWidth(0.5)
      .stroke();

    doc.restore();
    doc.y = prevY > 80 ? prevY : 80;
  };

  const drawFooter = () => {
    const prevY = doc.y;
    const prevMargins = { ...doc.page.margins };
    doc.page.margins.bottom = 0;

    const y = doc.page.height - 54;
    doc
      .save()
      .font('Helvetica')
      .fontSize(8.5)
      .fillColor('#3E5C8A')
      .text(
        'HART PROPERTY • Michael Hart | CEA Registration: R071893C | Agency License: L3008022J',
        48,
        y,
        { width: doc.page.width - 96, align: 'center' },
      )
      .restore();

    doc.page.margins.bottom = prevMargins.bottom;
    doc.y = prevY;
  };

  const drawTableRow = (
    label: string,
    value: string,
    options: { isHeader?: boolean; isLast?: boolean } = {},
  ) => {
    const { isHeader = false, isLast = false } = options;
    const startX = 48;
    const col1Width = 250;
    const col2Width = 250;
    const rowHeight = 22;

    ensureSpace(rowHeight);
    const drawY = doc.y;

    if (isHeader) {
      doc
        .save()
        .rect(startX, drawY, col1Width + col2Width, rowHeight)
        .fillColor('#F8FAFC')
        .fill()
        .restore();
    }

    doc
      .font(isHeader ? 'Helvetica-Bold' : 'Helvetica')
      .fontSize(10)
      .fillColor(isHeader ? '#13305D' : '#3E5C8A')
      .text(label, startX + 10, drawY + 6, { width: col1Width - 20 });

    doc.fillColor('#1F2933').text(value, startX + col1Width + 10, drawY + 6, {
      width: col2Width - 20,
      align: 'right',
    });

    // Bottom border
    if (!isLast) {
      doc
        .moveTo(startX, drawY + rowHeight)
        .lineTo(startX + col1Width + col2Width, drawY + rowHeight)
        .strokeColor('#E2E8F0')
        .lineWidth(0.5)
        .stroke();
    }

    doc.y = drawY + rowHeight;
    doc.x = startX;
  };

  doc.on('pageAdded', () => {
    drawHeader();
    drawFooter();
  });

  // First page setup
  drawHeader();
  drawFooter();

  // Title
  doc.moveDown(0.5);
  doc
    .font('Helvetica-Bold')
    .fontSize(20)
    .fillColor('#13305D')
    .text('Buying Plan Memo', { align: 'left' });

  doc
    .font('Helvetica')
    .fontSize(10)
    .fillColor('#3E5C8A')
    .text(
      `Generated: ${new Date().toLocaleString('en-SG', { timeZone: 'Asia/Singapore' })}`,
      { align: 'left' },
    );

  doc.moveDown(1.5);
  doc
    .font('Helvetica-Bold')
    .fontSize(12)
    .fillColor('#13305D')
    .text('Client Information', { align: 'left' });

  doc.moveDown(0.5);
  doc
    .font('Helvetica')
    .fontSize(10)
    .fillColor('#1F2933')
    .text(`Name: ${name?.trim() ? name.trim() : '-'}`, { align: 'left' })
    .text(`Email: ${email}`, { align: 'left' });

  // Selections
  doc.moveDown(1.5);
  doc
    .font('Helvetica-Bold')
    .fontSize(12)
    .fillColor('#13305D')
    .text('Your Selections', { align: 'left' });
  doc.moveDown(0.5);

  const selectionRows: Array<[string, string]> = [
    ['Buyer type', mapLabel(labelMaps.buyerType, selections.buyerType)],
    ['Age range', mapLabel(labelMaps.ageRange, selections.ageRange)],
    [
      'First private purchase',
      mapLabel(labelMaps.yesNo, selections.isFirstPurchase),
    ],
    [
      'Cash (downpayment + fees)',
      mapLabel(labelMaps.cashRange, selections.cashRange),
    ],
    ['CPF OA', mapLabel(labelMaps.cpfRange, selections.cpfRange)],
    [
      'Buffer (months)',
      mapLabel(labelMaps.bufferRange, selections.bufferRange),
    ],
    ['Target price', mapLabel(labelMaps.priceRange, selections.priceRange)],
    ['Location', mapLabel(labelMaps.locationPref, selections.locationPref)],
    ['New/resale', mapLabel(labelMaps.launchType, selections.launchType)],
    [
      'Good decision if flat 5y',
      mapLabel(labelMaps.yesNo, selections.isGoodDecision),
    ],
    [
      'Holding period',
      mapLabel(labelMaps.holdingPeriod, selections.holdingPeriod),
    ],
    ['Likely exit', mapLabel(labelMaps.exitOption, selections.exitOption)],
    [
      'Expected buyer at exit',
      mapLabel(labelMaps.targetBuyer, selections.targetBuyer),
    ],
    ['Income stability', mapLabel(labelMaps.stability, selections.stability)],
    [
      'Planned life changes',
      selections.lifeChanges
        ? mapLabel(
            labelMaps.lifeChanges as unknown as Record<string, string>,
            String(selections.lifeChanges ?? ''),
          )
        : '-',
    ],
  ];

  for (let i = 0; i < selectionRows.length; i++) {
    const [k, v] = selectionRows[i];
    drawTableRow(k, v || '-', { isLast: i === selectionRows.length - 1 });
  }

  // Market stats
  if (growthRegions.includes(selections.locationPref)) {
    const region = selections.locationPref as keyof typeof growth;
    doc.moveDown(1.5);
    doc
      .font('Helvetica-Bold')
      .fontSize(12)
      .fillColor('#13305D')
      .text('Market Context', { align: 'left' });

    doc.moveDown(0.5);
    doc
      .font('Helvetica')
      .fontSize(10)
      .fillColor('#3E5C8A')
      .text(
        'Non-landed private condos: Average annual PSF growth (2020–2023)',
        { align: 'left' },
      );

    doc.moveDown(0.2);
    doc
      .font('Helvetica')
      .fontSize(9)
      .fillColor('#3E5C8A')
      .text(
        'Note: Based on district-level average YoY PSF changes derived from private non-landed transactions between 2020 and 2023.',
        { align: 'left' },
      );

    doc.moveDown(0.2);
    doc
      .font('Helvetica-Bold')
      .fontSize(10)
      .fillColor('#1F2933')
      .text(
        `${mapLabel(labelMaps.locationPref, region)} Region: ${growth[region]}`,
        { align: 'left' },
      );
  }

  // Capital breakdown
  doc.moveDown(1.5);
  doc
    .font('Helvetica-Bold')
    .fontSize(12)
    .fillColor('#13305D')
    .text('Client Capital', { align: 'left' });

  doc.moveDown(0.3);
  doc
    .font('Helvetica')
    .fontSize(9)
    .fillColor('#3E5C8A')
    .text(
      'Assumptions: 25% downpayment (min 5% cash), excludes ABSD, legal/misc are estimates. Estimated monthly repayments use (1M SORA + 0.3%) to (6M SORA + 0.6%).',
      { align: 'left' },
    );

  const funds = cashCpfFromSelections(selections);
  doc.moveDown(0.5);

  drawTableRow('Cash', money(funds.cash));
  drawTableRow('CPF OA', money(funds.cpf), {
    isLast: true,
  });

  const sora = await fetchSoraSnapshot();

  const prices = pricePointsForRange(selections.priceRange);
  for (const p of prices) {
    doc.moveDown(1.2);
    const b = computeUpfront({
      price: p,
      cash: funds.cash,
      cpf: funds.cpf,
      ageRange: selections.ageRange,
    });

    doc
      .font('Helvetica-Bold')
      .fontSize(11)
      .fillColor('#13305D')
      .text(`Capital Check: ${money(p)} Analysis`, { align: 'left' });

    doc.moveDown(0.5);

    let repaymentRangeText: string | null = null;
    if (sora) {
      // Requirements:
      // - low: 1M SORA + 0.3% spread
      // - high: 6M SORA + 0.6% spread
      const lowAnnual = (sora.oneMonth + 0.3) / 100;
      const highAnnual = (sora.sixMonth + 0.6) / 100;
      const months = b.loanTenure * 12;

      const low = computeMonthlyRepayment({
        principal: b.maxLoan,
        annualRate: lowAnnual,
        months,
      });
      const high = computeMonthlyRepayment({
        principal: b.maxLoan,
        annualRate: highAnnual,
        months,
      });
      const lowFmt = money(Math.min(low, high));
      const highFmt = money(Math.max(low, high));
      repaymentRangeText = `${lowFmt} - ${highFmt}`;
    }

    const costLines: Array<[string, string]> = [
      ['Item', 'Amount'],
      [
        `Downpayment (${((b.downpaymentTotal / p) * 100).toFixed(1)}%)`,
        money(b.downpaymentTotal),
      ],
      [`Max Loan (${b.loanTenure} yrs @ 4.5% stress)`, money(b.maxLoan)],
      [
        'Estimated monthly repayments',
        repaymentRangeText ?? 'Unavailable (SORA fetch failed)',
      ],
      [`Buyer’s Stamp Duty (BSD) (~${b.bsdRate.toFixed(1)}%)`, money(b.bsd)],
      ['Legal fees (est.)', money(b.legalFees)],
      ['Other fees (est.)', money(b.miscFees)],
      ['Total upfront (est.)', money(b.totalUpfront)],
      ['CPF used (est.)', money(b.cpfUsed)],
      ['Cash Still Required', money(b.cashReqAfterCPF)],
      [
        'Cash remaining (est.)',
        b.cashShortfall > 0
          ? `Short by ${money(b.cashShortfall)}`
          : money(b.cashRemaining),
      ],
    ];

    for (let i = 0; i < costLines.length; i++) {
      const [k, v] = costLines[i];
      drawTableRow(k, v, {
        isHeader: i === 0,
        isLast: i === costLines.length - 1,
      });
    }
  }

  // Assessment
  doc.moveDown(1.5);
  const res = assess(selections, funds);
  doc
    .font('Helvetica-Bold')
    .fontSize(12)
    .fillColor('#13305D')
    .text('Assessment', { align: 'left' });

  doc.moveDown(0.5);
  doc
    .font('Helvetica')
    .fontSize(11)
    .fillColor(res.status === 'fail' ? '#B91C1C' : '#047857') // Stronger colors for status
    .text(res.message, {
      align: 'left',
    });

  // GPT Listings
  if (gptListings && gptListings.length > 0) {
    ensureSpace(120);
    doc.moveDown(2);
    doc
      .font('Helvetica-Bold')
      .fontSize(12)
      .fillColor('#13305D')
      .text('Based on your buyer profile these may suit you', {
        align: 'left',
      });

    doc.moveDown(0.75);

    for (const group of gptListings) {
      ensureSpace(100);
      doc
        .font('Helvetica-Bold')
        .fontSize(11)
        .fillColor('#13305D')
        .text(
          `Development: ${group.condo} | Range: ${group.price_lower}–${group.price_higher} | Size: ${group.size_lower}–${group.size_higher}`,
          { align: 'left' },
        );

      doc.moveDown(0.4);

      // Replace unit-level listings with a per-development CTA.
      const message = `I'm interested in a breakdown for condo ${group.condo}`;
      const waLink = `https://wa.me/6593490577?text=${encodeURIComponent(message)}`;

      const btnW = 240;
      const btnH = 28;
      const btnX = (doc.page.width - btnW) / 2;
      const btnY = doc.y;

      doc.save();
      doc.roundedRect(btnX, btnY, btnW, btnH, 8).fillColor('#25D366').fill();

      // Icon
      const iconSize = 16;
      const iconX = btnX + 14;
      const iconY = btnY + (btnH - iconSize) / 2;
      if (whatsappSvg) {
        doc.save();
        SVGtoPDF(doc as unknown as never, whatsappSvg, iconX, iconY, {
          width: iconSize,
          height: iconSize,
          preserveAspectRatio: 'xMidYMid meet',
        });
        doc.restore();
      } else {
        doc
          .font('Helvetica-Bold')
          .fontSize(9)
          .fillColor('#FFFFFF')
          .text('WA', iconX, iconY + 2, { width: iconSize, align: 'center' });
      }

      doc
        .font('Helvetica-Bold')
        .fontSize(10)
        .fillColor('#FFFFFF')
        .text('Request a breakdown', btnX, btnY + 8, {
          width: btnW,
          align: 'center',
          link: waLink,
        });

      // Make the entire button clickable.
      doc.link(btnX, btnY, btnW, btnH, waLink);
      doc.restore();

      doc.y = btnY + btnH;
      doc.moveDown(0.8);
      doc.x = 48; // Reset x to left margin after each development block
    }
  }

  // CTA
  doc.moveDown(2);
  doc.save();
  doc
    .rect(48, doc.y, doc.page.width - 96, 80)
    .fillColor('#F1F5F9')
    .fill();

  doc.y += 15;
  doc
    .font('Helvetica-Bold')
    .fontSize(12)
    .fillColor('#13305D')
    .text('Want a deeper analysis?', 60, undefined, {
      align: 'left',
    });

  doc.moveDown(0.3);
  doc
    .font('Helvetica')
    .fontSize(10)
    .fillColor('#1F2933')
    .text('WhatsApp: +65 9349 0577', 60, undefined, {
      align: 'left',
    })
    .text('Email: michael.hart@hartproperty.sg', 60, undefined, {
      align: 'left',
    });
  doc.restore();

  doc.end();
  return done;
}

const growth = {
  ocr: '+11.5%',
  rcr: '+5.8%',
  ccr: '+3.5%',
} as const;
const growthRegions = Object.keys(growth);

async function isValidPropertyGuruListingUrl(url: string): Promise<boolean> {
  console.log('[PG Validate] Start URL validation:', url);
  if (!url) {
    console.log('[PG Validate] ❌ Empty URL');
    return false;
  }
  // Only validate PropertyGuru URLs we expect to be navigable listing pages.
  if (!/^https:\/\/www\.propertyguru\.com\.sg\//i.test(url)) {
    console.log('[PG Validate] ❌ Not a propertyguru.com.sg URL');
    return false;
  }

  // Must include a numeric listing ID in the URL, otherwise treat as invalid.
  // Example: https://www.propertyguru.com.sg/listing/for-sale-parc-rosewood-60171136
  const listingIdMatch = url.match(/\/listing\/[^?#]*-(\d+)(?:[/?#]|$)/i);
  const listingId = listingIdMatch?.[1];
  if (!listingId) {
    console.log('[PG Validate] ❌ Missing listing ID in URL');
    return false;
  }

  const apiKey = process.env.GOOGLE_SEARCH_API_KEY;
  const cx =
    process.env.GOOGLE_SEARCH_ENGINE_ID ?? process.env.GOOGLE_SEARCH_CX;
  if (!apiKey || !cx) {
    console.log('[PG Validate] ❌ Missing Google Search env vars', {
      hasApiKey: Boolean(apiKey),
      hasCx: Boolean(cx),
    });
    return false;
  }

  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
  const normalizeUrl = (u: string) =>
    u
      .trim()
      .replace(/[?#].*$/, '')
      .replace(/\/$/, '')
      .toLowerCase();

  const want = normalizeUrl(url);
  // Use listingId for the query (site: expects a domain, not a full URL).
  // This is generally more reliable than searching for the exact URL string.
  const q = `site:${url}`;

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const endpoint =
        'https://www.googleapis.com/customsearch/v1' +
        `?key=${encodeURIComponent(apiKey)}` +
        `&cx=${encodeURIComponent(cx)}` +
        `&q=${encodeURIComponent(q)}` +
        `&num=5`;

      const res = await fetch(endpoint, {
        method: 'GET',
        headers: {
          Accept: 'application/json',
        },
      });

      if (!res.ok) {
        const shouldRetry =
          attempt < 3 && (res.status === 403 || res.status === 429);
        console.log('[PG Validate] Google Search API non-OK', {
          url,
          listingId,
          attempt,
          status: res.status,
          note: shouldRetry ? 'retrying' : 'not retrying',
        });
        if (shouldRetry) {
          await sleep(700 * attempt);
          continue;
        }
        return false;
      }

      const data = (await res.json()) as {
        items?: Array<{ link?: string }>;
      };
      const links = (data.items ?? [])
        .map((it) => it?.link)
        .filter((x): x is string => typeof x === 'string');

      const foundExact = links.some((l) => normalizeUrl(l) === want);
      const foundSameId = links.some((l) =>
        new RegExp(`-${listingId}(?:[/?#]|$)`, 'i').test(l),
      );

      console.log('[PG Validate] Google Search check', {
        url,
        listingId,
        results: links.length,
        foundExact,
        foundSameId,
      });

      // Treat as valid if there is any matching search result.
      return foundExact || foundSameId;
    } catch (err) {
      const e = err as { name?: unknown; message?: unknown };
      const message =
        typeof e?.message === 'string' ? e.message : 'Unknown error';
      const shouldRetry = attempt < 3;
      console.log('[PG Validate] Google Search API request failed', {
        url,
        listingId,
        attempt,
        name: typeof e?.name === 'string' ? e.name : undefined,
        message,
        note: shouldRetry ? 'retrying' : 'not retrying',
      });
      if (shouldRetry) {
        await sleep(700 * attempt);
        continue;
      }
      return false;
    }
  }

  return false;
}

async function validateGptListings(
  listings: GptListing[],
): Promise<GptListing[]> {
  const cleaned = listings
    .map((g) => ({
      ...g,
      condo: (g?.condo ?? '').trim(),
      listings: Array.isArray(g?.listings) ? g.listings : [],
    }))
    .filter((g) => Boolean(g.condo) && g.listings.length > 0);

  // Validate URLs and omit any that don't look like actual listing pages.
  // Keep concurrency small to avoid hammering the target site.
  const concurrency = 3;

  async function worker(
    group: GptListing,
    maxListingsForGroup: number,
  ): Promise<GptListing | null> {
    console.log('[PG Validate] Validating development:', group.condo);
    const items = group.listings;
    const out: typeof items = [];

    let i = 0;
    while (i < items.length) {
      if (out.length >= maxListingsForGroup) break;
      const chunk = items.slice(i, i + concurrency);
      const results = await Promise.all(
        chunk.map(async (it) => ({
          it,
          ok: await isValidPropertyGuruListingUrl(it.url),
        })),
      );
      for (const r of results) {
        if (out.length >= maxListingsForGroup) break;
        console.log('[PG Validate] URL result:', {
          condo: group.condo,
          url: r.it.url,
          ok: r.ok,
        });
        if (r.ok) out.push(r.it);
      }
      i += concurrency;
    }

    if (out.length === 0) return null;
    return { ...group, listings: out };
  }

  const validated: GptListing[] = [];
  let totalValid = 0;
  const maxTotalValid = 9;
  for (const group of cleaned) {
    const remaining = maxTotalValid - totalValid;
    if (remaining <= 0) break;
    // Intentionally sequential per development (with small concurrency per URL batch above)
    // to avoid hammering the target site.
    const v = await worker(group, remaining);
    if (v) {
      validated.push(v);
      totalValid += v.listings.length;
    }
  }

  return validated;
}

async function fetchGptListings(
  selections: ReviewPlanSelections,
): Promise<GptListing[]> {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) return [];

  // Important: On some hosts, a single long-lived await to the Responses API can appear to "hang"
  // indefinitely. We use Background Mode + polling with hard timeouts so this step cannot block
  // the job for hours.
  const client = new OpenAI({
    apiKey,
    // OpenAI Node SDK supports a request timeout (ms). Keep it bounded so network stalls don't hang the job.
    // (This is best-effort; if the SDK version changes, the polling deadline below is still the guardrail.)
    timeout: 15_000,
  });
  const priceLabel = mapLabel(
    labelMaps.priceRange as unknown as Record<string, string>,
    selections.priceRange,
  );

  const input = `can you search propertyguru for private condo sales in the range ${priceLabel}. find me listings from 5 condos. return listings in this JSON format with no other output, give me 3 listings per condo { condo: xxx, bedrooms: [2,3], price_lower: S$1.52M, price_higher: S$1.6M, size_lower: 600sqft, size_higher: 800 sqft, listings: [ { bedrooms: 2, price: $1.6M, size: 742sqft, url: https://xxxxx, ] } please make sure the URLs are to the actual listings and are navigatable, and not to the search results page. Return ONLY valid JSON array.`;

  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

  const extractOutputText = (resp: unknown): string | undefined => {
    if (!resp || typeof resp !== 'object') return undefined;
    const r = resp as Record<string, unknown>;

    const direct =
      r['output_text'] ??
      (r['output'] as any)?.[0]?.content ??
      (r['choices'] as any)?.[0]?.message?.content ??
      r['text'];

    return typeof direct === 'string' ? direct : undefined;
  };

  try {
    // Background Mode: returns quickly with an id; we then poll for completion.
    // Docs: https://platform.openai.com/docs/guides/background
    const created: any = await (client as any).responses.create({
      model: 'gpt-5',
      tools: [{ type: 'web_search' }],
      input,
      background: true,
    });

    const responseId: string | undefined =
      created?.id || created?.response_id || created?.response?.id;

    if (!responseId) {
      console.log(
        '[GPT Debug] Missing response id from background create:',
        JSON.stringify(created, null, 2),
      );
      return [];
    }

    const createdStatus = String(created?.status ?? '').toLowerCase();
    if (
      createdStatus &&
      createdStatus !== 'in_progress' &&
      createdStatus !== 'queued'
    ) {
      console.log(
        '[GPT Debug] Background create returned status:',
        createdStatus,
      );
    }

    const maxWaitMs = 60 * 60 * 1000; // 1 hour hard deadline for this step
    const start = Date.now();
    let attempt = 0;

    let finalResponse: unknown = undefined;
    while (Date.now() - start < maxWaitMs) {
      attempt += 1;

      const polled: any = await (client as any).responses.retrieve(responseId);
      const status = String(polled?.status ?? '').toLowerCase();

      if (status === 'completed') {
        finalResponse = polled;
        break;
      }

      if (
        status === 'failed' ||
        status === 'cancelled' ||
        status === 'expired'
      ) {
        console.log('[GPT Debug] Response ended early:', {
          responseId,
          status,
          error: polled?.error,
          incomplete_details: polled?.incomplete_details,
        });
        return [];
      }

      // Heartbeat log (throttled) to help debug server freezes.
      if (attempt === 1 || attempt % 5 === 0) {
        console.log('[GPT Debug] Polling response...', {
          responseId,
          status: status || 'unknown',
          elapsedMs: Date.now() - start,
        });
      }

      // Backoff with cap to keep load low for long-running jobs (2s → 30s)
      const delayMs = Math.min(30_000, 2000 + attempt * 1250);
      await sleep(delayMs);
    }

    if (!finalResponse) {
      console.log('[GPT Debug] Timed out waiting for response completion:', {
        responseId,
        elapsedMs: Date.now() - start,
      });
      return [];
    }

    console.log(
      '[GPT Debug] Final API Response:',
      JSON.stringify(finalResponse, null, 2),
    );

    const content = extractOutputText(finalResponse);
    console.log('[GPT Debug] Extracted content:', content);

    if (!content) return [];

    let parsed: any;
    if (typeof content === 'string') {
      // The model might return markdown, so we strip it
      const jsonStr = content.replace(/```json\n?|\n?```/g, '').trim();
      parsed = JSON.parse(jsonStr);
    } else {
      // If content is already an object (e.g. from a structured output tool)
      parsed = content;
    }

    let raw: GptListing[] = [];
    if (Array.isArray(parsed)) {
      raw = parsed as GptListing[];
    } else if (parsed?.listings && Array.isArray(parsed.listings)) {
      raw = parsed.listings as GptListing[];
    }
    if (raw.length === 0) return [];

    console.log('[GPT Debug] Raw listings parsed:', raw.length);
    const validated = await validateGptListings(raw);
    console.log(
      '[GPT Debug] Listings after URL validation:',
      validated.reduce((acc, g) => acc + g.listings.length, 0),
    );
    return validated;
  } catch (e) {
    console.error('Error fetching GPT listings:', e);
    // Fallback to empty if the beta API fails or is not available
    return [];
  }
}

@Injectable()
export class ReviewPlanService {
  constructor(
    @Inject(SUPABASE_CLIENT) private readonly supabase: SupabaseClient,
  ) {}

  async sendPlan(req: ReviewPlanRequest) {
    const email = (req.email ?? '').trim().toLowerCase();
    if (!email) {
      return { ok: false, error: 'Email is required.' } as const;
    }

    console.log(`[ReviewPlan] Starting plan request for: ${email}`);

    // Synchronous validation so UI knows if email is invalid
    const validation = await validateEmail(email, {
      preset: 'balanced',
      earlyExit: true,
      timeout: 3500,
      validators: {
        smtp: { enabled: false },
      },
    });

    const reason = validation.reason ?? 'invalid';
    if (!validation.valid && reason !== 'disposable') {
      console.log(
        `[ReviewPlan] Email validation failed for ${email} (reason=${reason})`,
      );
      const suggestion = validation.validators.typo?.error?.suggestion;
      return {
        ok: false,
        error: `Invalid email (${reason}).${suggestion ? ` Suggestion: ${suggestion}` : ''}`,
      } as const;
    }
    if (!validation.valid && reason === 'disposable') {
      console.log(
        `[ReviewPlan] Email flagged as disposable for ${email} — allowing request to continue`,
      );
    }

    // Persist submission immediately (so we store user selections even in serverless envs).
    let submissionId: string | null = null;
    const created = await this.supabase
      .from('review_plan_submissions')
      .insert({
        email,
        name: req.name ?? null,
        selections: req.selections,
        status: 'submitted',
      })
      .select('id')
      .maybeSingle();
    if (created.error) {
      console.log('[ReviewPlan] Failed to create submission record', {
        email,
        error: created.error.message,
      });
    } else {
      submissionId = created.data?.id ?? null;
      console.log('[ReviewPlan] Created submission record', {
        email,
        id: submissionId,
      });
    }

    // Start background job - don't await this
    this.runBackgroundGeneration(req, email, submissionId).catch((err) => {
      // Use stdout for visibility on platforms that don't capture stderr reliably.
      console.log(
        `[ReviewPlan] Critical error in background job for ${email}:`,
        err,
      );
    });

    console.log(
      `[ReviewPlan] Request accepted, generation running in background for ${email}`,
    );
    return { ok: true, message: 'Plan generation started.' } as const;
  }

  private async runBackgroundGeneration(
    req: ReviewPlanRequest,
    email: string,
    submissionId: string | null,
  ) {
    console.log(`[ReviewPlan] [${email}] Background job entered`);
    const postmarkKey = process.env.POSTMARK_API_KEY;
    const from = process.env.POSTMARK_FROM;

    if (!postmarkKey || !from) {
      console.log(`[ReviewPlan] [${email}] Missing Postmark env vars`, {
        hasPostmarkKey: Boolean(postmarkKey),
        hasFrom: Boolean(from),
      });
      throw new Error('Missing POSTMARK_API_KEY or POSTMARK_FROM');
    }

    console.log(`[ReviewPlan] [${email}] Step 1: Fetching GPT listings...`);
    const gptListings = await fetchGptListings(req.selections);
    console.log(
      `[ReviewPlan] [${email}] Step 1: GPT listings fetched (${gptListings.length} found)`,
    );

    // Persist all memo inputs/outputs to DB (including validated listing URLs),
    // even though we do not show unit-level listings in the PDF.
    const funds = cashCpfFromSelections(req.selections);
    const prices = pricePointsForRange(req.selections.priceRange);
    const sora = await fetchSoraSnapshot();
    const res = assess(req.selections, funds);
    const region = growthRegions.includes(req.selections.locationPref)
      ? (req.selections.locationPref as keyof typeof growth)
      : null;

    const capitalAnalyses = prices.map((p) => {
      const b = computeUpfront({
        price: p,
        cash: funds.cash,
        cpf: funds.cpf,
        ageRange: req.selections.ageRange,
      });

      let monthlyRepayments: { low: number; high: number } | null = null;
      if (sora) {
        const lowAnnual = (sora.oneMonth + 0.3) / 100;
        const highAnnual = (sora.sixMonth + 0.6) / 100;
        const months = b.loanTenure * 12;
        const low = computeMonthlyRepayment({
          principal: b.maxLoan,
          annualRate: lowAnnual,
          months,
        });
        const high = computeMonthlyRepayment({
          principal: b.maxLoan,
          annualRate: highAnnual,
          months,
        });
        monthlyRepayments = {
          low: Math.min(low, high),
          high: Math.max(low, high),
        };
      }

      return {
        price: p,
        breakdown: b,
        monthlyRepayments,
      };
    });

    const assessmentOk = res.status === 'pass';
    if (submissionId) {
      const updated = await this.supabase
        .from('review_plan_submissions')
        .update({
          market_context: region
            ? {
                region: region,
                regionLabel: mapLabel(labelMaps.locationPref, region),
                growth: growth[region],
                note: 'Non-landed private condos: Average annual PSF growth (2020–2023). Note: Based on district-level average YoY PSF changes derived from private non-landed transactions between 2020 and 2023.',
              }
            : null,
          client_capital: {
            funds,
            analyses: capitalAnalyses,
            assumptions:
              '25% downpayment (min 5% cash), excludes ABSD, legal/misc are estimates.',
          },
          assessment: {
            ok: assessmentOk,
            status: res.status,
            message: res.message,
          },
          listings: gptListings,
          sora_snapshot: sora,
          status: 'generated',
        })
        .eq('id', submissionId);
      if (updated.error) {
        console.log('[ReviewPlan] Failed to update submission record', {
          email,
          id: submissionId,
          error: updated.error.message,
        });
      }
    }

    console.log(`[ReviewPlan] [${email}] Step 2: Generating PDF memo...`);
    const pdf = await renderPdf({
      name: req.name,
      email,
      selections: req.selections,
      gptListings,
    });
    console.log(
      `[ReviewPlan] [${email}] Step 2: PDF generated (${pdf.length} bytes)`,
    );

    console.log(
      `[ReviewPlan] [${email}] Step 3: Sending email via Postmark...`,
    );
    const client = new postmark.ServerClient(postmarkKey);

    const subject = 'Your buying plan memo (Hart Property)';
    // Email clients frequently do not render SVGs. Use a PNG for email.
    const logoUrl = process.env.PUBLIC_ASSETS_BASE_URL
      ? `${process.env.PUBLIC_ASSETS_BASE_URL.replace(/\/$/, '')}/hart-logo.png`
      : 'https://hartproperty.sg/hart-logo.png';

    const selectionLines = [
      `Buyer type: ${mapLabel(labelMaps.buyerType, req.selections.buyerType)}`,
      `Age range: ${mapLabel(labelMaps.ageRange, req.selections.ageRange)}`,
      `First private purchase: ${mapLabel(labelMaps.yesNo, req.selections.isFirstPurchase)}`,
      `Cash: ${mapLabel(labelMaps.cashRange, req.selections.cashRange)}`,
      `CPF OA: ${mapLabel(labelMaps.cpfRange, req.selections.cpfRange)}`,
      `Buffer: ${mapLabel(labelMaps.bufferRange, req.selections.bufferRange)} months`,
      `Target price: ${mapLabel(labelMaps.priceRange, req.selections.priceRange)}`,
      `Location: ${mapLabel(labelMaps.locationPref, req.selections.locationPref)}`,
      `New/resale: ${mapLabel(labelMaps.launchType, req.selections.launchType)}`,
      `Holding period: ${mapLabel(labelMaps.holdingPeriod, req.selections.holdingPeriod)}`,
      `Likely exit: ${mapLabel(labelMaps.exitOption, req.selections.exitOption)}`,
      `Planned life changes: ${
        req.selections.lifeChanges
          ? mapLabel(
              labelMaps.lifeChanges as unknown as Record<string, string>,
              String(req.selections.lifeChanges ?? ''),
            )
          : '-'
      }`,
    ].filter(Boolean);

    const textBody =
      `Here is your buying plan memo (PDF attached).\n\n` +
      `Summary of your selections:\n- ${selectionLines.join('\n- ')}\n\n` +
      `You received this because you requested a memo on hartproperty.sg.\n` +
      `If this wasn’t you, reply to this email.\n\n` +
      `Unsubscribe (stop receiving these emails): mailto:unsubscribe@hartproperty.sg?subject=unsubscribe\n`;

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
              <div style="font-size:12px;color:#3E5C8A;">Buying plan memo</div>
            </div>
          </div>
        </div>

        <div style="padding:22px;font-family:Inter,Arial,sans-serif;color:#1F2933;line-height:1.55;">
          <div style="font-size:16px;font-weight:700;color:#13305D;margin-bottom:10px;">Your memo is attached (PDF)</div>
          <div style="font-size:14px;color:#3E5C8A;margin-bottom:18px;">
            Summary of your selections:
          </div>

          <ul style="margin:0 0 18px 18px;padding:0;font-size:14px;color:#1F2933;">
            ${selectionLines.map((l) => `<li style="margin:6px 0;">${l}</li>`).join('')}
          </ul>

          <div style="font-size:13px;color:#3E5C8A;margin-top:8px;">
            You received this because you requested a memo on
            <a href="https://hartproperty.sg" style="color:#4C7DBF;text-decoration:none;">hartproperty.sg</a>.
            If this wasn’t you, please reply to this email.
          </div>
        </div>

        <div style="padding:16px 22px;border-top:1px solid rgba(62,92,138,0.15);font-family:JetBrains Mono,ui-monospace,Menlo,Monaco,Consolas,monospace;font-size:11px;line-height:1.6;color:#3E5C8A;">
          <div>HART PROPERTY • Michael Hart | CEA Registration: R071893C | Agency License: L3008022J</div>
          <div style="margin-top:6px;">
            <a href="mailto:unsubscribe@hartproperty.sg?subject=unsubscribe" style="color:#4C7DBF;text-decoration:none;">Unsubscribe</a>
          </div>
        </div>
      </div>
    </div>
  </body>
</html>
`.trim();

    await client.sendEmail({
      MessageStream: 'outbound',
      From: `Hart Property <${from}>`,
      To: email,
      Bcc: 'michael.hart@hartproperty.sg',
      ReplyTo: `Hart Property <${from}>`,
      Subject: subject,
      TextBody: textBody,
      HtmlBody: htmlBody,
      Headers: [
        {
          Name: 'List-Unsubscribe',
          Value:
            '<mailto:unsubscribe@hartproperty.sg?subject=unsubscribe>, <https://hartproperty.sg/unsubscribe>',
        },
        { Name: 'List-Unsubscribe-Post', Value: 'List-Unsubscribe=One-Click' },
      ],
      Attachments: [
        {
          Name: 'HartProperty-BuyingPlan.pdf',
          Content: pdf.toString('base64'),
          ContentType: 'application/pdf',
          ContentID: null,
        },
      ],
    });

    console.log(`[ReviewPlan] [${email}] Step 3: Email sent successfully.`);
    console.log(`[ReviewPlan] [${email}] Finished background generation job.`);
  }
}
