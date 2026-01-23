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

async function extractDataFromImage(imagePath: string): Promise<any[]> {
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
- level: number (integer from the "Level" column)
- unit: string (the "Unit" number column, e.g., "12" or "05")
- unit_type: string (e.g., "2BR", "3BR", or null)
- sqft: number (integer from "Area (sqft)" column)
- price: number (integer from "Price" or "Sale (Price)" column, remove currency and commas)
- sale_type: string ("New Sale", "Resale", or "Sub Sale" from "Sale Type" column)

IMPORTANT: The "Street Name" column (e.g., "14 Adis Road") is NOT the level or unit. 
The "Level" and "Unit" columns are separate numeric columns. 
Ignore the street name.`,
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
    const data = JSON.parse(content);
    return data.transactions || data.rows || (Array.isArray(data) ? data : []);
  } catch (e) {
    console.error('  Failed to parse JSON:', content);
    return [];
  }
}

function parseDate(dateStr: string | null): string | null {
  if (!dateStr) return null;
  const d = new Date(dateStr.trim());
  if (isNaN(d.getTime())) return null;
  return d.toISOString().split('T')[0];
}

async function processTransaction(condoName: string, txn: any) {
  const saleDate = parseDate(txn.date);
  if (!saleDate || !txn.price) return null;

  const salePrice = txn.price;
  const exactLevel = txn.level;
  const exactUnit = txn.unit;
  const saleMonth = parseInt(saleDate.substring(0, 4) + saleDate.substring(5, 7), 10);
  const condoNameLower = condoName.toLowerCase();

  // Resale Logic: Find previous purchase
  let purchasePrice: number | null = null;
  let purchaseDate: string | null = null;
  let profit: number | null = null;
  let annualisedPct: number | null = null;

  if (txn.sale_type === 'Resale' || txn.sale_type === 'Sub Sale') {
    const { data: prevSale } = await supabase
      .from('condo_sale_transactions')
      .select('sale_price, sale_date')
      .eq('condo_name_lower', condoNameLower)
      .eq('exact_level', exactLevel)
      .eq('exact_unit', exactUnit)
      .lt('sale_date', saleDate)
      .order('sale_date', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (prevSale) {
      purchasePrice = prevSale.sale_price;
      purchaseDate = prevSale.sale_date;
      if (purchasePrice !== null) {
        profit = salePrice - purchasePrice;
        const d1 = new Date(purchaseDate!).getTime();
        const d2 = new Date(saleDate).getTime();
        const years = (d2 - d1) / (1000 * 60 * 60 * 24 * 365.25);
        if (years > 0) {
          annualisedPct = (Math.pow(salePrice / purchasePrice, 1 / years) - 1) * 100;
          annualisedPct = Math.round(annualisedPct * 100) / 100;
        }
      }
    }
  }

  return {
    condo_name: condoName,
    condo_name_lower: condoNameLower,
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
  const files = fs.readdirSync(folderPath).filter(f => 
    ['.png', '.jpg', '.jpeg'].includes(path.extname(f).toLowerCase())
  );

  console.log(`Found ${files.length} images. Results saved per-image to ${csvPath}`);

  let allResults: any[] = [];

  for (const file of files) {
    const filePath = path.join(folderPath, file);
    try {
      const transactions = await extractDataFromImage(filePath);
      console.log(`    Extracted ${transactions.length} rows`);
      
      for (const txn of transactions) {
        const processed = await processTransaction(condoName, txn);
        if (processed) {
          allResults.push(processed);
          // Insert to DB immediately
          const { error } = await supabase
            .from('condo_sale_transactions')
            .upsert(processed, { 
              onConflict: 'condo_name_lower,sale_price,sale_month,exact_level,exact_unit',
              ignoreDuplicates: false // We want to update existing rows with precise level/unit if they were URA rows
            });
          if (error) console.log(`    DB Skip/Error: ${processed.exact_level}-${processed.exact_unit} | ${error.message}`);
        }
      }

      // Update CSV after every image
      const csvData = stringify(allResults, { header: true });
      fs.writeFileSync(csvPath, csvData);
      console.log(`    [Progress] CSV updated. Total rows: ${allResults.length}`);

    } catch (err) {
      console.error(`Error processing ${file}:`, err);
    }
  }

  console.log('\nAll done! Final CSV at:', csvPath);
}

main();
