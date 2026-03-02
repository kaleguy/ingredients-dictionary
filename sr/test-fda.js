const FoodNutrientFetcher = require('./FoodNutrientFetcher');

async function getFoodInfo(searchQuery) {
    try {
        const fetcher = new FoodNutrientFetcher();

        // First search for foods and show options
        const searchResults = await fetcher.searchFoods(searchQuery);
        console.log('Search results:');
        searchResults.forEach((food, index) => {
            console.log(`${index + 1}. ${food.description} (${food.dataType})`);
        });

        // Get detailed nutrients for the first result
        // You could modify this to let the user choose which result they want
        if (searchResults.length > 0) {
            const nutrients = await fetcher.getFoodNutrients(searchResults[0].fdcId);
            console.log('\nNutrient information:');
            console.log(JSON.stringify(nutrients, null, 2));
        }
    } catch (error) {
        console.error('Error:', error.message);
    }
}

getFoodInfo('Sunflower Seeds lb');