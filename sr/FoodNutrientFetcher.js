require('dotenv').config();
const https = require('https');
const { Pool } = require('pg');

config = require('../../config');

const pgConfig = {
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false
    }
};

const pool = new Pool(pgConfig);

class FoodNutrientFetcher {
    constructor() {
        this.apiKey = process.env.USDA_API_KEY;
        if (!this.apiKey) {
            throw new Error('USDA_API_KEY is not set in environment variables');
        }
        this.baseUrl = 'api.nal.usda.gov';
    }

    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async processIngredientBatch() {
        try {
            const query = `
                SELECT id, name 
                FROM list_ingredients 
                WHERE fda_name IS NULL 
                LIMIT 3
            `;
            const { rows } = await pool.query(query);

            if (rows.length === 0) {
                console.log('No more ingredients to process');
                return;
            }

            console.log(`Processing ${rows.length} ingredients...`);

            for (const ingredient of rows) {
                try {
                    console.log(`\nProcessing: ${ingredient.name}`);

                    // Search for matches
                    const searchResults = await this.searchFoods(ingredient.name);
                    console.log(`Found ${Array.isArray(searchResults) ? searchResults.length : 0} matches`);

                    // Wait 2 seconds between search and getting nutrients
                    await this.delay(2000);

                    if (Array.isArray(searchResults) && searchResults.length > 0) {
                        // Get detailed nutrients for the first result
                        console.log(`Getting nutrients for: ${searchResults[0].description}`);
                        const nutrients = await this.getFoodNutrients(searchResults[0].fdcId);

                        if (nutrients.error) {
                            console.error(`Error getting nutrients: ${nutrients.error}`);
                            continue;
                        }

                        // Prepare the data
                        const fdaName = searchResults[0].description;
                        const fdaNames = searchResults.map(r => r.description);

                        // Update the database
                        await pool.query(
                            `UPDATE list_ingredients 
                             SET fda_name = $1, 
                                 fda_names = $2, 
                                 fda_nutrients = $3 
                             WHERE id = $4`,
                            [fdaName, fdaNames, nutrients, ingredient.id]
                        );

                        console.log(`Successfully updated: ${ingredient.name} → ${fdaName}`);

                        // Wait 3 seconds between ingredients
                        await this.delay(3000);
                    } else {
                        console.log(`No matches found for: ${ingredient.name}`);
                        // Still wait between ingredients even if no match found
                        await this.delay(2000);
                    }
                } catch (error) {
                    console.error(`Error processing ${ingredient.name}:`, error);
                    // Wait if there's an error before continuing
                    await this.delay(2000);
                }
            }

            console.log('\nBatch processing complete');
        } catch (error) {
            console.error('Batch processing error:', error);
        }
    }

    async searchFoods(query, pageSize = 5) {
        try {
            console.log(`Searching for: "${query}"`);
            const searchData = await this.makeRequest({
                path: '/fdc/v1/foods/search',
                params: {
                    api_key: this.apiKey,
                    query: query,
                    pageSize: pageSize,
                    dataType: "SR Legacy,Foundation",
                    sortBy: "dataType.keyword",
                    sortOrder: "asc",
                    requireAllWords: true
                }
            });

            if (!searchData.foods || searchData.foods.length === 0) {
                return { error: 'No food found matching the query' };
            }

            return searchData.foods.map(food => ({
                fdcId: food.fdcId,
                description: food.description,
                dataType: food.dataType,
                category: food.foodCategory
            }));
        } catch (error) {
            console.error('Search error:', error);
            return { error: `Failed to search foods: ${error.message}` };
        }
    }

    async getFoodNutrients(fdcId) {
        try {
            const foodData = await this.makeRequest({
                path: `/fdc/v1/food/${fdcId}`,
                params: {
                    api_key: this.apiKey,
                    format: 'full'
                }
            });

            // Process and format the nutrition data
            const nutrients = {};
            if (foodData.foodNutrients) {
                foodData.foodNutrients.forEach(nutrient => {
                    if (nutrient.nutrient && nutrient.amount) {
                        nutrients[nutrient.nutrient.name] = {
                            amount: nutrient.amount,
                            unit: nutrient.nutrient.unitName
                        };
                    }
                });
            }

            // Get portions information if available
            const portions = foodData.foodPortions?.map(portion => ({
                amount: portion.amount,
                unit: portion.measureUnit?.name,
                gramWeight: portion.gramWeight
            })) || [];

            return {
                description: foodData.description || '',
                category: foodData.foodCategory?.description || '',
                brandOwner: foodData.brandOwner || '',
                portions: portions,
                nutrients: nutrients,
                dataType: foodData.dataType,
                publicationDate: foodData.publicationDate
            };

        } catch (error) {
            console.error('Nutrient fetch error:', error);
            return { error: `Failed to fetch food data: ${error.message}` };
        }
    }

    makeRequest({ path, params }) {
        return new Promise((resolve, reject) => {
            const queryString = new URLSearchParams(params).toString();
            const options = {
                hostname: this.baseUrl,
                path: `${path}?${queryString}`,
                method: 'GET',
                headers: {
                    'Accept': 'application/json'
                }
            };

            const req = https.request(options, (res) => {
                let data = '';

                res.on('data', (chunk) => {
                    data += chunk;
                });

                res.on('end', () => {
                    try {
                        const parsedData = JSON.parse(data);
                        if (res.statusCode >= 400) {
                            reject(new Error(parsedData.error || 'API request failed'));
                        } else {
                            resolve(parsedData);
                        }
                    } catch (error) {
                        reject(new Error('Failed to parse JSON response'));
                    }
                });
            });

            req.on('error', (error) => {
                reject(error);
            });

            req.end();
        });
    }
}

async function main() {
    try {
        const fetcher = new FoodNutrientFetcher();
        await fetcher.processIngredientBatch();
    } catch (error) {
        console.error('Main error:', error);
    } finally {
        await pool.end(); // Make sure to close the database connection
        console.log('Database connection closed');
    }
}

// Run if this file is being run directly
if (require.main === module) {
    main().catch(console.error);
}

module.exports = FoodNutrientFetcher;