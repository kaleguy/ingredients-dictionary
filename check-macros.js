/**
 * Check CSV ingredient macros against FDA (USDA FoodData Central) database.
 *
 * For each row in ingredients.csv:
 *   1. Search the FDA API for the food
 *   2. Get per-100g nutrient data
 *   3. Scale to the serving size gram weight
 *   4. Compare with the CSV values
 *   5. Output a new CSV with fda_* correction columns where values differ
 *
 * Usage: node check-macros.js
 * Requires: USDA_API_KEY in .env
 */

require('dotenv').config();
const fs = require('fs');
const https = require('https');
const path = require('path');

const API_KEY = process.env.USDA_API_KEY;
if (!API_KEY) {
  console.error('Missing USDA_API_KEY in .env');
  process.exit(1);
}

// --- CSV parsing (handles quoted fields with commas) ---
function parseCSVLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        current += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        fields.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  fields.push(current);
  return fields;
}

function readCSV(filePath) {
  const text = fs.readFileSync(filePath, 'utf-8').trim();
  const lines = text.split('\n');
  const headers = parseCSVLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = parseCSVLine(lines[i]);
    const row = {};
    headers.forEach((h, idx) => {
      row[h.trim()] = vals[idx]?.trim() ?? '';
    });
    rows.push(row);
  }
  return { headers, rows };
}

// Extract gram weight from serving size like "1 medium (182g)" or "1 cup (240ml)"
function extractGrams(servingSize) {
  // Match grams
  const gMatch = servingSize.match(/\((\d+\.?\d*)g\)/i);
  if (gMatch) return parseFloat(gMatch[1]);
  // For ml-based (milk, oil), treat ml ≈ g as rough approximation
  const mlMatch = servingSize.match(/\((\d+\.?\d*)ml\)/i);
  if (mlMatch) return parseFloat(mlMatch[1]);
  return null;
}

// --- FDA API helpers ---
function requestJSON(urlPath, params) {
  const qs = new URLSearchParams(params).toString();
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'api.nal.usda.gov',
      path: `${urlPath}?${qs}`,
      method: 'GET',
      headers: { Accept: 'application/json' },
    }, (res) => {
      let data = '';
      res.on('data', c => (data += c));
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (res.statusCode >= 400) return reject(new Error(parsed?.error || `HTTP ${res.statusCode}`));
          resolve(parsed);
        } catch {
          reject(new Error('Failed to parse JSON'));
        }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function searchFood(query) {
  const res = await requestJSON('/fdc/v1/foods/search', {
    api_key: API_KEY,
    query,
    pageSize: 10,
    dataType: 'Foundation,SR Legacy',
    sortBy: 'dataType.keyword',
    sortOrder: 'asc',
  });
  return res?.foods || [];
}

async function getFoodNutrients(fdcId) {
  return requestJSON(`/fdc/v1/food/${fdcId}`, {
    api_key: API_KEY,
    format: 'full',
  });
}

// Extract per-100g macros from FDA food data
// Nutrient IDs: 1008=Energy(kcal), 1003=Protein, 1005=Carbs, 1079=Fiber
function extractMacrosPer100g(food) {
  const nutrients = food.foodNutrients || [];
  const byId = new Map();
  nutrients.forEach(fn => {
    const id = fn.nutrient?.id;
    if (id != null && fn.amount != null) byId.set(id, fn.amount);
  });
  return {
    calories: byId.get(1008) ?? null,
    protein: byId.get(1003) ?? null,
    carbs: byId.get(1005) ?? null,
    fiber: byId.get(1079) ?? null,
  };
}

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

// Strip qualifiers from name for searching, e.g. "Black beans, cooked" -> "Black beans"
// but keep it full for better matching first
function buildSearchQueries(name) {
  const queries = [name];
  // Also try without cooking state
  const stripped = name.replace(/,\s*(cooked|raw|dry|canned|plain|firm|boneless skinless|deli|whole wheat|all-purpose|80% lean|2%|whole|brown|white|Atlantic|sweet|granulated|canned in water)\s*/gi, ' ').trim();
  if (stripped !== name) queries.push(stripped);
  return queries;
}

function round1(n) {
  return n == null ? null : Math.round(n * 10) / 10;
}

// Tolerance for comparison: values within this % are considered matching
const TOLERANCE_PCT = 15;

function isClose(csvVal, fdaVal) {
  if (csvVal === '' || csvVal == null) return true; // CSV is blank, nothing to check
  if (fdaVal == null) return true; // FDA has no data
  const csv = parseFloat(csvVal);
  if (isNaN(csv)) return true;
  if (csv === 0 && fdaVal === 0) return true;
  const diff = Math.abs(csv - fdaVal);
  const base = Math.max(Math.abs(csv), Math.abs(fdaVal), 1);
  return (diff / base) <= (TOLERANCE_PCT / 100);
}

async function main() {
  const csvPath = path.join(__dirname, 'ingredients.csv');
  const { headers, rows } = readCSV(csvPath);

  const results = [];
  let matchCount = 0;
  let mismatchCount = 0;
  let errorCount = 0;

  for (const row of rows) {
    const name = row.name;
    const grams = extractGrams(row.servingSize);
    const result = { ...row, fda_name: '', fda_calories: '', fda_protein: '', fda_carbs: '', fda_fiber: '', status: '' };

    if (!grams) {
      console.log(`[SKIP] ${name}: could not extract gram weight from "${row.servingSize}"`);
      result.status = 'NO_GRAM_WEIGHT';
      results.push(result);
      errorCount++;
      continue;
    }

    const queries = buildSearchQueries(name);
    let food = null;
    let bestMatch = null;

    for (const q of queries) {
      try {
        console.log(`Searching: "${q}"`);
        const foods = await searchFood(q);
        if (foods.length > 0) {
          bestMatch = foods[0];
          break;
        }
        await delay(300);
      } catch (e) {
        console.error(`  Search error for "${q}": ${e.message}`);
      }
    }

    if (!bestMatch) {
      console.log(`[NOT FOUND] ${name}`);
      result.status = 'NOT_FOUND';
      results.push(result);
      errorCount++;
      await delay(500);
      continue;
    }

    try {
      await delay(400);
      food = await getFoodNutrients(bestMatch.fdcId);
    } catch (e) {
      console.error(`  Nutrient fetch error for ${name}: ${e.message}`);
      result.status = 'FETCH_ERROR';
      results.push(result);
      errorCount++;
      await delay(500);
      continue;
    }

    const per100 = extractMacrosPer100g(food);
    const scale = grams / 100;

    const fdaCals = round1(per100.calories != null ? per100.calories * scale : null);
    const fdaProtein = round1(per100.protein != null ? per100.protein * scale : null);
    const fdaCarbs = round1(per100.carbs != null ? per100.carbs * scale : null);
    const fdaFiber = round1(per100.fiber != null ? per100.fiber * scale : null);

    result.fda_name = food.description || bestMatch.description;

    const calsOk = isClose(row.calories, fdaCals);
    const proteinOk = isClose(row.protein, fdaProtein);
    const carbsOk = isClose(row.carbs, fdaCarbs);
    const fiberOk = isClose(row.fiber, fdaFiber);

    const allOk = calsOk && proteinOk && carbsOk && fiberOk;

    if (allOk) {
      result.status = 'OK';
      matchCount++;
      console.log(`[OK] ${name} -> ${result.fda_name}`);
    } else {
      result.status = 'MISMATCH';
      result.fda_calories = fdaCals ?? '';
      result.fda_protein = fdaProtein ?? '';
      result.fda_carbs = fdaCarbs ?? '';
      result.fda_fiber = fdaFiber ?? '';
      mismatchCount++;

      const diffs = [];
      if (!calsOk) diffs.push(`cal: ${row.calories || '?'} vs ${fdaCals}`);
      if (!proteinOk) diffs.push(`pro: ${row.protein || '?'} vs ${fdaProtein}`);
      if (!carbsOk) diffs.push(`carb: ${row.carbs || '?'} vs ${fdaCarbs}`);
      if (!fiberOk) diffs.push(`fiber: ${row.fiber || '?'} vs ${fdaFiber}`);
      console.log(`[MISMATCH] ${name} -> ${result.fda_name}: ${diffs.join(', ')}`);
    }

    results.push(result);
    await delay(500);
  }

  // Write output CSV
  const outHeaders = [...headers, 'fda_name', 'fda_calories', 'fda_protein', 'fda_carbs', 'fda_fiber', 'status'];
  const outLines = [outHeaders.join(',')];
  for (const r of results) {
    const vals = outHeaders.map(h => {
      const v = r[h] ?? '';
      // Quote if contains comma
      return String(v).includes(',') ? `"${v}"` : String(v);
    });
    outLines.push(vals.join(','));
  }

  const outPath = path.join(__dirname, 'ingredients-checked.csv');
  fs.writeFileSync(outPath, outLines.join('\n') + '\n');

  console.log(`\n--- Summary ---`);
  console.log(`Total: ${rows.length}`);
  console.log(`OK: ${matchCount}`);
  console.log(`Mismatches: ${mismatchCount}`);
  console.log(`Errors/Skipped: ${errorCount}`);
  console.log(`Output written to: ${outPath}`);
}

main().catch(e => {
  console.error('Fatal:', e);
  process.exit(1);
});
