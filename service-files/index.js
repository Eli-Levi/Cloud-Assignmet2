const express = require('express');
const AWS = require('aws-sdk');
const RestaurantsMemcachedActions = require('./model/restaurantsMemcachedActions');
const app = express();
app.use(express.json());

const MEMCACHED_CONFIGURATION_ENDPOINT = process.env.MEMCACHED_CONFIGURATION_ENDPOINT;
const TABLE_NAME = process.env.TABLE_NAME;
const AWS_REGION = process.env.AWS_REGION;
const USE_CACHE = process.env.USE_CACHE === 'true';

const memcachedActions = new RestaurantsMemcachedActions(MEMCACHED_CONFIGURATION_ENDPOINT);

// Create a new DynamoDB instance
const dynamodb = new AWS.DynamoDB.DocumentClient({ region: AWS_REGION });

app.get('/', (req, res) => {
    const response = {
        MEMCACHED_CONFIGURATION_ENDPOINT: MEMCACHED_CONFIGURATION_ENDPOINT,
        TABLE_NAME: TABLE_NAME,
        AWS_REGION: AWS_REGION,
        //USE_CACHE: USE_CACHE ---> NOTE: Missing in original test. 
    };
    res.send(response);
});

app.post('/restaurants', async (req, res) => {
    const restaurant = req.body;

    if (!restaurant.name || !restaurant.cuisine || !restaurant.region) {
        return res.status(400).send({ success: false, message: 'Some fields are missing' });
    }

    try {
        // Check if the restaurant exists in Memcached
        const cachedRestaurant = await memcachedActions.getRestaurants(restaurant.name);

        if (cachedRestaurant) {
            // If found in cache, return a conflict error
            return res.status(409).send({ success: false, message: 'Restaurant already exists' });
        }

        // Check if the restaurant exists in DynamoDB
        const getParams = {
            TableName: TABLE_NAME,
            Key: { RestaurantNameKey: restaurant.name }
        };

        const result = await dynamodb.get(getParams).promise();

        if (result.Item) {
            // If found in DB, add it to Memcached and return a conflict error
            await memcachedActions.addRestaurants(restaurant.name, result.Item);
            return res.status(409).send({ success: false, message: 'Restaurant already exists' });
        }

        // Restaurant does not exist in DB or Memcached, add it to the table
        const putParams = {
            TableName: TABLE_NAME,
            Item: {
                RestaurantNameKey: restaurant.name,
                cuisine: restaurant.cuisine,
                GeoRegion: restaurant.region,
                rating: restaurant?.rating || 0 // Takes given rating if exists or else, sets rating to default rating of 0
            }
        };

        await dynamodb.put(putParams).promise();

        // Add the new restaurant to Memcached
        await memcachedActions.addRestaurants(restaurant.name, putParams.Item);

        res.status(200).json({ success: true });
    } catch (error) {
        console.error('Error adding restaurant:', error);
        res.status(500).send('Internal Server Error');
    }
});


app.get('/restaurants/:restaurantName', async (req, res) => {
    const restaurantName = req.params.restaurantName;

    // Input validation
    if (!restaurantName) {
        return res.status(400).send({ message: 'Restaurant name is required' });
    }

    try {
        // Check if the restaurant exists in Memcached
        const cachedRestaurantResult = await memcachedActions.getRestaurants(restaurantName);
        // Restaurant found, format the data
        const formattedCachedRestaurantResult = {
            name: cachedRestaurantResult.RestaurantNameKey,
            cuisine: cachedRestaurantResult.cuisine,
            rating: cachedRestaurantResult.rating || 0,
            region: cachedRestaurantResult.GeoRegion
        };

        if (cachedRestaurantResult) {
            // Return the restaurant details from the cache
            return res.status(200).json(formattedCachedRestaurantResult);
        }

        // If not found in cache, check in DynamoDB
        const parameters = {
            TableName: TABLE_NAME,
            Key: {
                RestaurantNameKey: restaurantName
            }
        };

        // Perform the fetch operation
        const result = await dynamodb.get(parameters).promise();

        if (!result.Item) {
            // Restaurant not found or undefined
            return res.status(404).send({ message: 'Restaurant not found' });
        }

        // Restaurant found, format the data
        const restaurant = {
            name: result.Item.RestaurantNameKey,
            cuisine: result.Item.cuisine,
            rating: result.Item.rating || 0,
            region: result.Item.GeoRegion
        };

        // Store the restaurant details in Memcached
        await memcachedActions.addRestaurants(restaurantName, restaurant);

        // Return the restaurant details
        res.status(200).json(restaurant);
    } catch (error) {
        console.error('Error retrieving restaurant:', error);
        res.status(500).send('Internal Server Error');
    }
});

// double check delete logic
app.delete('/restaurants/:restaurantName', async (req, res) => {
    const restaurantName = req.params.restaurantName;

    // Input validation
    if (!restaurantName) {
        return res.status(400).send({ message: 'Restaurant name is required' });
    }

    try {
        // Check if the restaurant exists in Memcached
        const cachedRestaurant = await memcachedActions.getRestaurants(restaurantName);

        if (cachedRestaurant) {
            // If found in cache, delete from Memcached
            await memcachedActions.deleteRestaurants(restaurantName);
        }

        // Check if the restaurant exists in DynamoDB
        const del_param = {
            TableName: TABLE_NAME,
            Key: {
                RestaurantNameKey: restaurantName
            }
        };

        const result = await dynamodb.get(del_param).promise();

        if (result.Item) {
            // Perform the delete operation in DynamoDB
            await dynamodb.delete(del_param).promise();

            // Return success message
            return res.status(200).json({ success: true });
        } else {
            // Restaurant not found
            return res.status(404).send({ message: 'No such restaurant exists to delete' });
        }
    } catch (error) {
        console.error('Error deleting restaurant:', error);
        res.status(500).send('Internal Server Error');
    }
});


app.post('/restaurants/rating', async (req, res) => {
    const restaurantName = req.body.name;
    const newRating = req.body.rating;

    if (!restaurantName || !newRating) {
        console.error('POST /restaurants/rating', 'Missing required fields');
        res.status(400).send({ success: false, message: 'Missing required fields' });
        return;
    }

    // Fetch restaurant from the DynamoDB table
    const paramaters = {
        TableName: TABLE_NAME,
        Key: {
            RestaurantNameKey: restaurantName
        }
    };

    try {
        // Perform the query operation to find the restaurant by name
        const result = await dynamodb.get(paramaters).promise();

        if (result.Item) {
            const restaurant = result.Item;

            // Calculate the new average rating
            const currentRating = restaurant.rating || 0;
            const ratingCount = restaurant.rating_count || 0;

            const newRatingCount = ratingCount + 1;
            const newAverageRating = ((currentRating * ratingCount) + newRating) / newRatingCount;

            // Define the parameters for updating the item in the table
            const updateParams = {
                TableName: TABLE_NAME,
                Key: {
                    RestaurantNameKey: restaurantName
                },
                UpdateExpression: 'set rating = :rating, rating_count = :count',
                ExpressionAttributeValues: {
                    ':rating': newAverageRating,
                    ':count': newRatingCount
                }
            };

            // Perform the update operation in DynamoDB
            await dynamodb.update(updateParams).promise();

            // If caching is enabled, update the restaurant in Memcached
            if (USE_CACHE) {
                restaurant.rating = newAverageRating;
                restaurant.rating_count = newRatingCount;
                await memcachedActions.addRestaurants(restaurantName, restaurant);
            }

            // Return success message
            res.status(200).json({ success: true });
        } else {
            // Restaurant not found
            res.status(404).send({ message: 'Restaurant not found' });
        }
    } catch (error) {
        console.error('Error adding rating:', error);
        res.status(500).send('Internal Server Error');
    }
});


app.get('/restaurants/cuisine/:cuisine', async (req, res) => {
    const cuisine = req.params.cuisine;
    let limit = parseInt(req.query.limit, 10) || 10;
    const minimum_rating = parseFloat(req.query.minRating) || 0;

    if (limit > 100) {
        limit = 100;
    }

    if (!cuisine) {
        console.error('GET /restaurants/cuisine/:cuisine', 'Missing required fields');
        return res.status(400).send({ success: false, message: 'Missing required fields' });
    }

    const cacheKey = `cuisine:${cuisine}:limit:${limit}:minRating:${minimum_rating}`;
    let cachedRestaurants = null;

    if (USE_CACHE) {
        cachedRestaurants = await memcachedActions.getRestaurants(cacheKey);
        if (cachedRestaurants) {
            console.log('Returning data from cache');
            return res.status(200).json(cachedRestaurants);
        }
    }

    const queryParams = {
        TableName: TABLE_NAME,
        IndexName: 'CuisineRatingIndex',
        KeyConditionExpression: 'cuisine = :cuisine',
        ExpressionAttributeValues: {
            ':cuisine': cuisine
        },
        ScanIndexForward: false,
        Limit: limit
    };

    try {
        const result = await dynamodb.query(queryParams).promise();

        const filteredRestaurants = result.Items.filter(item => item.rating >= minimum_rating)
            .map(item => ({
                name: item.RestaurantNameKey,
                cuisine: item.cuisine,
                rating: item.rating,
                region: item.GeoRegion
            }));

        if (filteredRestaurants.length > 0) {
            if (USE_CACHE) {
                await memcachedActions.addRestaurants(cacheKey, filteredRestaurants);
            }
            res.status(200).json(filteredRestaurants);
        } else {
            res.status(404).send({ message: 'No restaurants found for this cuisine' });
        }
    } catch (error) {
        console.error('Error retrieving top-rated restaurants:', error);
        res.status(500).send('Internal Server Error');
    }
});


app.get('/restaurants/region/:region', async (req, res) => {
    const region = req.params.region;
    let limit = parseInt(req.query.limit, 10) || 10;
    const minimum_rating = parseFloat(req.query.minRating) || 0;

    if (limit > 100) {
        limit = 100;
    }

    if (!region) {
        console.error('GET /restaurants/region/:region', 'Missing required fields');
        return res.status(400).send({ success: false, message: 'Missing required fields' });
    }

    const cacheKey = `region:${region}:limit:${limit}:minRating:${minimum_rating}`;
    
    try {
        let cachedResults = null;

        if (USE_CACHE) {
            cachedResults = await memcachedActions.getRestaurants(cacheKey);
        }

        if (cachedResults) {
            console.log('Cache hit, returning cached results.');
            return res.status(200).json(cachedResults);
        }

        const queryParams = {
            TableName: TABLE_NAME,
            IndexName: 'GeoRegionRatingIndex',
            KeyConditionExpression: 'GeoRegion = :region',
            ExpressionAttributeValues: {
                ':region': region
            },
            ScanIndexForward: false,
            Limit: limit
        };

        const result = await dynamodb.query(queryParams).promise();

        const filteredRestaurants = result.Items.filter(item => item.rating >= minimum_rating)
            .map(item => ({
                name: item.RestaurantNameKey,
                cuisine: item.cuisine,
                rating: item.rating,
                region: item.GeoRegion
            }));

        if (filteredRestaurants.length > 0) {
            if (USE_CACHE) {
                await memcachedActions.addRestaurants(cacheKey, filteredRestaurants);
            }
            return res.status(200).json(filteredRestaurants);
        } else {
            return res.status(404).send({ message: 'No restaurants found for this region' });
        }
    } catch (error) {
        console.error('Error retrieving top-rated restaurants:', error);
        res.status(500).send('Internal Server Error');
    }
});



app.get('/restaurants/region/:region/cuisine/:cuisine', async (req, res) => {
    const region = req.params.region;
    const cuisine = req.params.cuisine;
    const minRating = parseFloat(req.query.minRating) || 0;
    let limit = parseInt(req.query.limit) || 10;

    if (limit > 100) {
        limit = 100;
    }

    if (!region || !cuisine) {
        return res.status(400).send({ message: 'Both region and cuisine are required' });
    }

    const cacheKey = `region:${region}:cuisine:${cuisine}:limit:${limit}:minRating:${minRating}`;

    try {
        if (USE_CACHE) {
            // Check if the result is in Memcached
            const cachedData = await memcachedActions.getRestaurants(cacheKey);
            if (cachedData) {
                console.log('Returning data from cache');
                return res.status(200).json(JSON.parse(cachedData));
            }
        }

        // Query DynamoDB
        const queryParams = {
            TableName: TABLE_NAME,
            IndexName: 'GeoCuisineIndex',
            KeyConditionExpression: 'GeoRegion = :region AND cuisine = :cuisine',
            FilterExpression: 'rating >= :minRating',
            ExpressionAttributeValues: {
                ':region': region,
                ':cuisine': cuisine,
                ':minRating': minRating
            },
            ScanIndexForward: false, // Descending order by rating
            Limit: limit
        };

        const result = await dynamodb.query(queryParams).promise();

        if (result.Items && result.Items.length > 0) {
            const formattedItems = result.Items.map(item => ({
                name: item.RestaurantNameKey,
                cuisine: item.cuisine,
                rating: item.rating,
                region: item.GeoRegion
            }));

            if (USE_CACHE) {
                try {
                    // Update Memcached with the new data
                    await memcachedActions.addRestaurants(cacheKey, JSON.stringify(formattedItems), 3600); // Cache for 1 hour

                } catch (cacheError) {
                    console.error('Error adding to memcached:', cacheError);
                }
            }

            res.status(200).json(formattedItems);
        } else {
            res.status(404).send({ message: 'No restaurants found for the specified region, cuisine, and rating' });
        }
    } catch (error) {
        console.error('Error retrieving restaurants:', error.stack);
        res.status(500).send('Internal Server Error');
    }
});


app.listen(3001, () => {
    console.log('Server is running on http://localhost:80');
});


module.exports = { app };
