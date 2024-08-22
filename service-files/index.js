const express = require('express');
const RestaurantsMemcachedActions = require('./model/restaurantsMemcachedActions');
const AWS = require('aws-sdk');
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

    // Check if the restaurant already exists
    const getParams = {
        TableName: TABLE_NAME,
        Key: { RestaurantNameKey: restaurant.name }
    };

    try {
        const result = await dynamodb.get(getParams).promise();

        if (result.Item) {
            return res.status(409).send({ success: false, message: 'Restaurant already exists' });
        }

        // Restaurant does not exist, add it to the table
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

    // Check if the restaurant exists in the DynamoDB table
    const parameters = {
        TableName: TABLE_NAME,
        Key: {
            RestaurantNameKey: restaurantName
        }
    };

    try {
        // Perform the fetch operation
        const result = await dynamodb.get(parameters).promise();

        if (!result.Item) {
            // Restaurant not found or undefined
            return res.status(404).send({ message: 'Restaurant not found' });
        }

        // Restaurant found, return the details as a flat object
        const restaurant = {
            name: result.Item.RestaurantNameKey,
            cuisine: result.Item.cuisine,
            rating: result.Item.rating || 0,
            region: result.Item.GeoRegion
        };

        res.status(200).json(restaurant);
    } catch (error) {
        console.error('Error retrieving restaurant:', error);
        res.status(500).send('Internal Server Error');
    }
});


app.delete('/restaurants/:restaurantName', async (req, res) => {
    const restaurantName = req.params.restaurantName;

    // Input validation
    if (!restaurantName) {
        return res.status(400).send({ message: 'Restaurant name is required' });
    }

    // Check if the restaurant exists in the DynamoDB table
    const del_param = {
        TableName: TABLE_NAME,
        Key: {
            RestaurantNameKey: restaurantName
        }
    };

    try {
        // Perform the query operation to find the restaurant by name
        const result = await dynamodb.get(del_param).promise();

        
        if (result.Items && result.Items.length > 0) {

            // Perform the delete operation
            await dynamodb.delete(del_param).promise();

            // Return success message
            res.status(200).json({ success: true });
        } else {
            // Restaurant not found
            res.status(404).send({ message: 'No such restaurant exists to delete' });
        }
    } catch (error) {
        console.error('Error deleting restaurant:', error);
        res.status(500).send('Internal Server Error');
    }
});


app.post('/restaurants/rating', async (req, res) => {
    const restaurantName = req.body.name;
    const newRating = req.body.rating;

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

            // Perform the update operation
            await dynamodb.update(updateParams).promise();

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

    // Ensure the limit is within the acceptable range
    if (limit > 100) {
        limit = 100;
    }

    // Input validation
    if (!cuisine) {
        console.error('GET /restaurants/cuisine/:cuisine', 'Missing required fields');
        res.status(400).send({ success: false, message: 'Missing required fields' });
        return;
    }
    
    // Define the parameters for querying the DynamoDB table using the GSI
    const queryParams = {
        TableName: TABLE_NAME,
        IndexName: 'CuisineRatingIndex', // The name of the GSI
        KeyConditionExpression: 'cuisine = :cuisine',
        ExpressionAttributeValues: {
            ':cuisine': cuisine
        },
        ScanIndexForward: false, // This ensures that results are sorted in descending order by rating
        Limit: limit
    };

    try {
        // Perform the query operation to get the top-rated restaurants by cuisine
        const result = await dynamodb.query(queryParams).promise();

        // Filter results based on minRating
        const filteredRestaurants = result.Items.filter(item => item.rating >= minimum_rating);

        if (filteredRestaurants.length > 0) {
            // Return the list of top-rated restaurants
            res.status(200).json(filteredRestaurants);
        } else {
            // No restaurants found for the given cuisine
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

    // Ensure the limit is within the acceptable range
    if (limit > 100) {
        limit = 100;
    }

    // Input validation
    if (!region) {
        console.error('GET /restaurants/region/:region', 'Missing required fields');
        res.status(400).send({ success: false, message: 'Missing required fields' });
        return;
    }

    // Define the parameters for querying the DynamoDB table using the GSI
    const queryParams = {
        TableName: TABLE_NAME,
        IndexName: 'GeoRegionRatingIndex', // The name of the GSI
        KeyConditionExpression: 'GeoRegion = :region',
        ExpressionAttributeValues: {
            ':region': region
        },
        ScanIndexForward: false, // This ensures that results are sorted in descending order by rating
        Limit: limit
    };

    try {
        // Perform the query operation to get the top-rated restaurants by region
        const result = await dynamodb.query(queryParams).promise();

        // Filter results based on if not using FilterExpression in DynamoDB query
        const filteredRestaurants = result.Items.filter(item => item.rating >= minimum_rating);

        if (filteredRestaurants.length > 0) {
            // Return the list of top-rated restaurants
            res.status(200).json(filteredRestaurants);
        } else {
            // No restaurants found for the given region
            res.status(404).send({ message: 'No restaurants found for this region' });
        }
    } catch (error) {
        console.error('Error retrieving top-rated restaurants:', error);
        res.status(500).send('Internal Server Error');
    }
});


app.get('/restaurants/region/:region/cuisine/:cuisine', async (req, res) => {
    const region = req.params.region;
    const cuisine = req.params.cuisine;
    let limit = parseInt(req.query.limit) || 10;
    const minimum_rating = parseFloat(req.query.minRating) || 0; // Default to 0 if no minimum rating is provided

    // Ensure the limit is within the acceptable range
    if (limit > 100) {
        limit = 100;
    }

    // Input validation
    if (!region || !cuisine) {
        return res.status(400).send({ message: 'Both region and cuisine are required' });
    }

    // Define the parameters for querying the DynamoDB table using the GSI
    const queryParams = {
        TableName: TABLE_NAME,
        IndexName: 'GeoCuisineIndex', // The name of the GSI
        KeyConditionExpression: 'GeoRegion = :region AND cuisine = :cuisine',
        FilterExpression: 'rating >= :minRating', // Filter by minimum rating
        ExpressionAttributeValues: {
            ':region': region,
            ':cuisine': cuisine,
            ':minRating': minRating
        },
        ScanIndexForward: false, // Sorts results by rating in descending order
        Limit: limit
    };

    try {
        // Perform the query operation
        const result = await dynamodb.query(queryParams).promise();

        if (result.Items && result.Items.length > 0) {
            // Restaurants found, return the details
            res.status(200).json(result.Items); // correct to json form
        } else {
            // No restaurants found
            res.status(404).send({ message: 'No restaurants found for the specified region, cuisine, and rating' });
        }
    } catch (error) {
        console.error('Error retrieving restaurants:', error);
        res.status(500).send('Internal Server Error');
    }
});


app.listen(80, () => {
    console.log('Server is running on http://localhost:80');
});

module.exports = { app };