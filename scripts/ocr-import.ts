import { createClient } from '@supabase/supabase-js';
import * as dotenv from 'dotenv';
import * as fs from 'fs';
import * as path from 'path';
import OpenAI from 'openai';
import { stringify } from 'csv-stringify/sync';

dotenv.config();

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_API_KEY!,
);

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

interface ExtractedTransaction {
  date: string | null;
  level: number | null;
  unit: number | null; // Integer unit number (e.g., 5 for #12-05)
  unit_type: string | null;
  property_type: string | null;
  sqft: number | null;
  price: number | null;
  sale_type: string | null;
}

interface ProcessedTransaction {
  condo_name: string;
  condo_name_lower: string;
  property_type: string | null;
  sale_date: string;
  sale_price: number;
  sale_month: number;
  sqft: number | null;
  unit_type: string | null;
  exact_level: number | null;
  exact_unit: number | null; // Integer unit number
  type_of_sale: string | null;
  purchase_price: number | null;
  purchase_date: string | null;
  profit: number | null;
  annualised_pct: number | null;
}

interface ExtractedData {
  transactions?: ExtractedTransaction[];
  rows?: ExtractedTransaction[];
}

async function extractDataFromImage(
  imagePath: string,
): Promise<ExtractedTransaction[]> {
  const base64Image = fs.readFileSync(imagePath, { encoding: 'base64' });
  const extension = path.extname(imagePath).toLowerCase().replace('.', '');
  const mimeType = extension === 'png' ? 'image/png' : 'image/jpeg';

  console.log(`  [Vision] Extracting data from ${path.basename(imagePath)}...`);

  const response = await openai.chat.completions.create({
    model: 'gpt-4o',
    messages: [
      {
        role: 'system',
        content: `You are a specialized data extractor for property transactions from PropNex/URA screenshots. 
Extract all table rows from the image. 
Return ONLY a JSON object with a key "transactions" containing an array of objects.
Each object must have these keys:
- date: string (the "Contract Date" or "Date of Sale", e.g., "23 Dec 2025")
- level: number (integer - the FLOOR number, e.g., for "#12-05" the level is 12)
- unit: number (integer - the UNIT number AFTER the dash, e.g., for "#12-05" the unit is 5, NOT "05")
- unit_type: string (e.g., "2BR", "3BR", or null if not shown)
- property_type: null (ALWAYS return null - we get this from URA data, DO NOT guess or infer)
- sqft: number (integer from "Area (sqft)" column)
- price: number (integer from "Price" or "Sale (Price)" column, remove currency and commas)
- sale_type: string ("New Sale", "Resale", or "Sub Sale" from "Sale Type" column, or null if not shown)

IMPORTANT: The "Street Name" column (e.g., "14 Adis Road") is NOT the level or unit. 
IMPORTANT: level is the FLOOR (first number), unit is the UNIT NUMBER (second number after dash). Return as integers, not strings.
IMPORTANT: property_type must ALWAYS be null - never guess or infer it!`,
      },
      {
        role: 'user',
        content: [
          {
            type: 'image_url',
            image_url: {
              url: `data:${mimeType};base64,${base64Image}`,
            },
          },
        ],
      },
    ],
    response_format: { type: 'json_object' },
  });

  const content = response.choices[0].message.content;
  if (!content) return [];

  try {
    const data = JSON.parse(content) as ExtractedData | ExtractedTransaction[];
    if (Array.isArray(data)) {
      return data;
    }
    return data.transactions || data.rows || [];
  } catch {
    console.error('  Failed to parse JSON:', content);
    return [];
  }
}

function parseDate(dateStr: string | null): string | null {
  if (!dateStr) return null;
  const str = dateStr.trim();

  // Try format: "23 Dec 2025" or "23 December 2025"
  const monthNames: Record<string, string> = {
    jan: '01',
    january: '01',
    feb: '02',
    february: '02',
    mar: '03',
    march: '03',
    apr: '04',
    april: '04',
    may: '05',
    jun: '06',
    june: '06',
    jul: '07',
    july: '07',
    aug: '08',
    august: '08',
    sep: '09',
    september: '09',
    oct: '10',
    october: '10',
    nov: '11',
    november: '11',
    dec: '12',
    december: '12',
  };

  // Match "DD Mon YYYY" or "DD Month YYYY"
  const match = str.match(/^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$/);
  if (match) {
    const day = match[1].padStart(2, '0');
    const monthKey = match[2].toLowerCase();
    const month = monthNames[monthKey];
    const year = match[3];
    if (month) {
      return `${year}-${month}-${day}`;
    }
  }

  // Try format: "YYYY-MM-DD" (already correct)
  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
    return str;
  }

  // Try format: "DD/MM/YYYY"
  const slashMatch = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (slashMatch) {
    const day = slashMatch[1].padStart(2, '0');
    const month = slashMatch[2].padStart(2, '0');
    const year = slashMatch[3];
    return `${year}-${month}-${day}`;
  }

  return null;
}

async function processTransaction(
  condoName: string,
  txn: ExtractedTransaction,
): Promise<ProcessedTransaction | null> {
  const saleDate = parseDate(txn.date);
  if (!saleDate || !txn.price) return null;

  const salePrice = txn.price;
  const exactLevel = txn.level;
  const exactUnit = txn.unit;
  const saleMonth = parseInt(
    saleDate.substring(0, 4) + saleDate.substring(5, 7),
    10,
  );
  const condoNameLower = condoName.toLowerCase();

  // Resale Logic: Find previous purchase
  let purchasePrice: number | null = null;
  let purchaseDate: string | null = null;
  let profit: number | null = null;
  let annualisedPct: number | null = null;

  if (txn.sale_type === 'Resale' || txn.sale_type === 'Sub Sale') {
    const { data: prevSale } = await supabase
      .from('sale_transactions')
      .select('sale_price, sale_date')
      .eq('condo_name_lower', condoNameLower)
      .eq('exact_level', exactLevel)
      .eq('exact_unit', exactUnit)
      .lt('sale_date', saleDate)
      .order('sale_date', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (prevSale) {
      purchasePrice = prevSale.sale_price as number | null;
      purchaseDate = prevSale.sale_date as string | null;
      if (purchasePrice !== null && purchaseDate !== null) {
        profit = salePrice - purchasePrice;
        const d1 = new Date(purchaseDate).getTime();
        const d2 = new Date(saleDate).getTime();
        const years = (d2 - d1) / (1000 * 60 * 60 * 24 * 365.25);
        if (years > 0) {
          annualisedPct =
            (Math.pow(salePrice / purchasePrice, 1 / years) - 1) * 100;
          annualisedPct = Math.round(annualisedPct * 100) / 100;
        }
      }
    }
  }

  return {
    condo_name: condoName,
    condo_name_lower: condoNameLower,
    property_type: txn.property_type || null,
    sale_date: saleDate,
    sale_price: salePrice,
    sale_month: saleMonth,
    sqft: txn.sqft,
    unit_type: txn.unit_type,
    exact_level: exactLevel,
    exact_unit: exactUnit,
    type_of_sale: txn.sale_type,
    purchase_price: purchasePrice,
    purchase_date: purchaseDate,
    profit: profit,
    annualised_pct: annualisedPct,
  };
}

async function main() {
  const args = process.argv.slice(2);
  if (args.length < 2) {
    console.log('Usage: npm run ingest:ocr <folder_path> "<condo_name>"');
    process.exit(1);
  }

  const folderPath = path.resolve(args[0]);
  const condoName = args[1];
  const csvPath = path.join(folderPath, 'extracted_transactions.csv');
  const files = fs
    .readdirSync(folderPath)
    .filter((f) =>
      ['.png', '.jpg', '.jpeg'].includes(path.extname(f).toLowerCase()),
    );

  console.log(
    `Found ${files.length} images. Results saved per-image to ${csvPath}`,
  );

  const allResults: ProcessedTransaction[] = [];

  for (const file of files) {
    const filePath = path.join(folderPath, file);
    try {
      const transactions = await extractDataFromImage(filePath);
      console.log(`    Extracted ${transactions.length} rows`);

      for (const txn of transactions) {
        const processed = await processTransaction(condoName, txn);
        if (processed) {
          allResults.push(processed);
          // Insert to DB - use RPC to preserve existing purchase_price/purchase_date
          const { error } = await supabase.rpc('upsert_ocr_transaction', {
            p_condo_name: processed.condo_name,
            p_condo_name_lower: processed.condo_name_lower,
            p_sale_date: processed.sale_date,
            p_sale_price: processed.sale_price,
            p_sale_month: processed.sale_month,
            p_sqft: processed.sqft,
            p_unit_type: processed.unit_type,
            p_exact_level: processed.exact_level,
            p_exact_unit: processed.exact_unit,
            p_type_of_sale: processed.type_of_sale,
            p_property_type: processed.property_type,
          });
          if (error) {
            console.log(
              `    DB Skip/Error: ${processed.exact_level}-${processed.exact_unit} | ${error.message}`,
            );
          }
        }
      }

      // Update CSV after every image
      const csvData = stringify(allResults, { header: true });
      fs.writeFileSync(csvPath, csvData);
      console.log(
        `    [Progress] CSV updated. Total rows: ${allResults.length}`,
      );
    } catch (err) {
      console.error(`Error processing ${file}:`, err);
    }
  }

  console.log('\nAll done! Final CSV at:', csvPath);
}

void main();
