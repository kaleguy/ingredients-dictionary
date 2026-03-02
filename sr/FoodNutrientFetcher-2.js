/**
 * USDA exact-match ingester
 * - Reads list_ingredients_usda(search_name)
 * - Searches FDC and ONLY accepts exact description matches (case-insensitive)
 * - Preference: Foundation > SR Legacy > Survey (FNDDS) > Branded
 * - Populates per-100 g nutrient columns + fdc_id, fda_name, usda_data_type, usda_publication_date
 *
 * ENV:
 *   DATABASE_URL
 *   USDA_API_KEY
 */

require('dotenv').config();
const https = require('https');
const { Pool } = require('pg');

// --- SSL: enable only for common cloud hosts or NODE_ENV=production
const isProd =
    process.env.NODE_ENV === 'production' ||
    /@(.*\.)?(amazonaws\.com|herokuapp\.com|render\.com|supabase\.co|timescaledb\.cloud|neon\.tech)/i.test(
        process.env.DATABASE_URL || ''
    );

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: isProd ? { rejectUnauthorized: false } : false,
});

class USDAExactMatch {
  constructor() {
    this.apiKey = process.env.USDA_API_KEY;
    if (!this.apiKey) throw new Error('USDA_API_KEY is not set');
    this.baseHost = 'api.nal.usda.gov';
    this.priority = ['Foundation', 'SR Legacy', 'Survey (FNDDS)', 'Branded'];
  }

  delay(ms) { return new Promise(r => setTimeout(r, ms)); }

  // --- MAIN LOOP ------------------------------------------------------------
  async runBatch(limit = 10) {
    const selectSql = `
      SELECT id, search_name
      FROM list_ingredients_usda
      WHERE (fdc_id IS NULL OR calories_kcal IS NULL)
        AND COALESCE(TRIM(search_name), '') <> ''
      AND manual IS NOT TRUE
      ORDER BY id
      LIMIT $1
    `;
    const { rows } = await pool.query(selectSql, [limit]);
    if (!rows.length) {
      console.log('Nothing to do.');
      return;
    }

    console.log(`Processing ${rows.length} rows…`);
    for (const row of rows) {
      const q = (row.search_name || '').trim();
      try {
        console.log(`\n🔎 Searching exact: "${q}"`);

        // Single broad search; we will filter to exact matches only.
        const results = await this.searchAll(q, 60);
        const exacts = this.filterExactByDescription(results, q);

        if (!exacts.length) {
          console.log(`⛔ No EXACT match for id=${row.id} ("${q}"). Skipping so you can fix search_name.`);
          continue;
        }

        // Choose best exact by dataType priority
        const chosenMeta =
            exacts.find(f => f.dataType === this.priority[0]) ||
            exacts.find(f => f.dataType === this.priority[1]) ||
            exacts.find(f => f.dataType === this.priority[2]) ||
            exacts[0];

        console.log(`➡️ Exact match: ${chosenMeta.description} [${chosenMeta.dataType}] (fdcId=${chosenMeta.fdcId})`);

        await this.delay(400);
        const food = await this.getFood(chosenMeta.fdcId);

        // Optional: ensure core nutrients present
        if (!this.hasCoreNutrients(food)) {
          console.log(`⚠️ Exact match lacks core nutrients; skipping id=${row.id}.`);
          continue;
        }

        await this.updateRow(row.id, chosenMeta, food);
        console.log(`✅ Updated id=${row.id}`);
        await this.delay(600);
      } catch (e) {
        console.error(`❌ Error on id=${row.id}: ${e.message}`);
        await this.delay(300);
      }
    }

    console.log('\nBatch complete.');
  }

  // --- SEARCH (broad) then FILTER (exact) -----------------------------------
  async searchAll(query, pageSize = 200) {
    const attempts = [
      { requireAllWords: true },
      { requireAllWords: false },
    ];
    const dataType = 'Foundation,SR Legacy,Survey (FNDDS),Branded';

    for (const a of attempts) {
      const res = await this.requestJSON('/fdc/v1/foods/search', {
        api_key: this.apiKey,
        query,
        pageSize,                  // larger window to catch exact
        dataType,
        sortBy: 'dataType.keyword',
        sortOrder: 'asc',
        requireAllWords: a.requireAllWords,
      }).catch(() => null);

      const foods = res?.foods || [];
      if (foods.length) {
        return foods.map(f => ({
          fdcId: f.fdcId,
          description: f.description,
          dataType: f.dataType,
          foodCategory: f.foodCategory,
        }));
      }
      await this.delay(150);
    }
    return [];
  }


  filterExactByDescription(results, query) {
    const norm = (s) => (s ?? '')
        .toLowerCase()
        .normalize('NFKC')               // unify unicode forms
        .replace(/\u00a0/g, ' ')         // no-break spaces -> space
        .replace(/[’‘]/g, "'")           // smart quotes -> ascii
        .replace(/[“”]/g, '"')
        .replace(/\s*,\s*/g, ',')        // normalize comma spacing
        .replace(/\s+/g, ' ')            // collapse internal spaces
        .trim();

    const bare = (s) => norm(s).replace(/[^a-z0-9]/g, ''); // ignore punctuation

    const qNorm  = norm(query);
    const qBare  = bare(query);

    return results.filter(r => {
      const dNorm = norm(r.description);
      if (dNorm === qNorm) return true;       // strict normalized equality
      const dBare = bare(r.description);
      return dBare === qBare;                 // punctuation-insensitive equality
    });
  }


  async getFood(fdcId) {
    return this.requestJSON(`/fdc/v1/food/${fdcId}`, {
      api_key: this.apiKey,
      format: 'full',
    });
  }

  requestJSON(path, params) {
    const qs = new URLSearchParams(params).toString();
    const options = {
      hostname: 'api.nal.usda.gov',
      path: `${path}?${qs}`,
      method: 'GET',
      headers: { Accept: 'application/json' },
    };

    return new Promise((resolve, reject) => {
      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', (c) => (data += c));
        res.on('end', () => {
          try {
            const parsed = JSON.parse(data);
            if (res.statusCode >= 400) {
              return reject(new Error(parsed?.error || `HTTP ${res.statusCode}`));
            }
            resolve(parsed);
          } catch {
            reject(new Error('Failed to parse JSON response'));
          }
        });
      });
      req.on('error', reject);
      req.end();
    });
  }

  // Require basic nutrients so bad survey/branded rows get ignored
  hasCoreNutrients(food) {
    const ids = new Set((food.foodNutrients || []).map(n => n.nutrient?.id));
    return [1008, 1003, 1004, 1005, 1093].every(id => ids.has(id)); // kcal, protein, fat, carbs, sodium
  }

  mapNutrients(food) {
    const out = {
      calories_kcal: null,
      total_fat_g: null,
      sat_fat_g: null,
      trans_fat_g: null,
      cholesterol_mg: null,
      sodium_mg: null,
      total_carbohydrate_g: null,
      dietary_fiber_g: null,
      total_sugars_g: null,
      added_sugars_g: null,
      protein_g: null,
      vitamin_d_mcg: null,
      calcium_mg: null,
      iron_mg: null,
      potassium_mg: null,
      vitamin_a_mcg_rae: null,
      vitamin_c_mg: null,
    };

    const idMap = {
      1008: 'calories_kcal',          // Energy (kcal)
      1004: 'total_fat_g',            // Total lipid (fat)
      1258: 'sat_fat_g',              // Fatty acids, total saturated
      1257: 'trans_fat_g',            // Fatty acids, total trans
      1253: 'cholesterol_mg',         // Cholesterol
      1093: 'sodium_mg',              // Sodium
      1005: 'total_carbohydrate_g',   // Carbohydrate, by difference
      1079: 'dietary_fiber_g',        // Fiber, total dietary
      2000: 'total_sugars_g',         // Sugars, total including NLEA
      1235: 'added_sugars_g',         // Sugars, added
      1003: 'protein_g',              // Protein
      1114: 'vitamin_d_mcg',          // Vitamin D (mcg)
      1087: 'calcium_mg',             // Calcium
      1089: 'iron_mg',                // Iron
      1092: 'potassium_mg',           // Potassium
      1106: 'vitamin_a_mcg_rae',      // Vitamin A, RAE
      1162: 'vitamin_c_mg',           // Vitamin C
    };

    const byId = new Map();
    const byName = new Map();
    (food.foodNutrients || []).forEach(fn => {
      const id = fn.nutrient?.id ?? fn.nutrient?.number;
      const name = fn.nutrient?.name?.toLowerCase().trim();
      const amount = fn.amount ?? null;
      if (id != null) byId.set(Number(id), amount);
      if (name) byName.set(name, amount);
    });

    // preferred by ID
    for (const [fdcId, col] of Object.entries(idMap)) {
      const amt = byId.get(Number(fdcId));
      if (amt != null) out[col] = amt;
    }

    // minimal name fallbacks
    const nameFallbacks = [
      ['energy (kcal)', 'calories_kcal'],
      ['total lipid (fat)', 'total_fat_g'],
      ['fatty acids, total saturated', 'sat_fat_g'],
      ['fatty acids, total trans', 'trans_fat_g'],
      ['cholesterol', 'cholesterol_mg'],
      ['sodium, na', 'sodium_mg'],
      ['carbohydrate, by difference', 'total_carbohydrate_g'],
      ['fiber, total dietary', 'dietary_fiber_g'],
      ['sugars, total including nlea', 'total_sugars_g'],
      ['sugars, added', 'added_sugars_g'],
      ['protein', 'protein_g'],
      ['vitamin d (d2 + d3), mcg', 'vitamin_d_mcg'],
      ['calcium, ca', 'calcium_mg'],
      ['iron, fe', 'iron_mg'],
      ['potassium, k', 'potassium_mg'],
      ['vitamin a, rae', 'vitamin_a_mcg_rae'],
      ['vitamin c, total ascorbic acid', 'vitamin_c_mg'],
    ];
    for (const [needle, col] of nameFallbacks) {
      if (out[col] == null && byName.has(needle)) out[col] = byName.get(needle);
    }

    return out;
  }

  async updateRow(id, chosen, food) {
    const mapped = this.mapNutrients(food);
    const pubDate = food.publicationDate ? food.publicationDate.slice(0, 10) : null;

    const sql = `
      UPDATE list_ingredients_usda
      SET fdc_id = $1,
          fda_name = $2,
          usda_data_type = $3,
          usda_publication_date = $4,

          calories_kcal = $5,
          total_fat_g = $6,
          sat_fat_g = $7,
          trans_fat_g = $8,
          cholesterol_mg = $9,
          sodium_mg = $10,
          total_carbohydrate_g = $11,
          dietary_fiber_g = $12,
          total_sugars_g = $13,
          added_sugars_g = $14,
          protein_g = $15,
          vitamin_d_mcg = $16,
          calcium_mg = $17,
          iron_mg = $18,
          potassium_mg = $19,
          vitamin_a_mcg_rae = $20,
          vitamin_c_mg = $21
      WHERE id = $22
    `;

    const vals = [
      chosen.fdcId,
      food.description || chosen.description || null,
      food.dataType || chosen.dataType || null,
      pubDate,

      mapped.calories_kcal,
      mapped.total_fat_g,
      mapped.sat_fat_g,
      mapped.trans_fat_g,
      mapped.cholesterol_mg,
      mapped.sodium_mg,
      mapped.total_carbohydrate_g,
      mapped.dietary_fiber_g,
      mapped.total_sugars_g,
      mapped.added_sugars_g,
      mapped.protein_g,
      mapped.vitamin_d_mcg,
      mapped.calcium_mg,
      mapped.iron_mg,
      mapped.potassium_mg,
      mapped.vitamin_a_mcg_rae,
      mapped.vitamin_c_mg,

      id,
    ];

    await pool.query(sql, vals);
  }
}

// --- entrypoint -------------------------------------------------------------
async function main() {
  const worker = new USDAExactMatch();
  try {
    await worker.runBatch(10); // tune batch size as you like
  } catch (e) {
    console.error('Fatal:', e.message);
  } finally {
    await pool.end();
    console.log('DB connection closed.');
  }
}

if (require.main === module) main();
module.exports = USDAExactMatch;
