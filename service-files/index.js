const express = require('express');
const RestaurantsMemcachedActions = require('./model/restaurantsMemcachedActions');

const app = express();
app.use(express.json());

const MEMCACHED_CONFIGURATION_ENDPOINT = process.env.MEMCACHED_CONFIGURATION_ENDPOINT;
const TABLE_NAME = process.env.TABLE_NAME;
const AWS_REGION = process.env.AWS_REGION;
const USE_CACHE = process.env.USE_CACHE === 'true';

const memcachedActions = new RestaurantsMemcachedActions(MEMCACHED_CONFIGURATION_ENDPOINT);

app.get('/', (req, res) => {
    const response = {
        MEMCACHED_CONFIGURATION_ENDPOINT: MEMCACHED_CONFIGURATION_ENDPOINT,
        TABLE_NAME: TABLE_NAME,
        AWS_REGION: AWS_REGION,
        USE_CACHE: USE_CACHE
    };
    res.send(response);
});

app.post('/restaurants', async (req, res) => {
    const AWS = require('aws-sdk');
    const dynamodb = new AWS.DynamoDB.DocumentClient();

    const restaurant = req.body;
    const { restaurant_id } = restaurant; // Assuming restaurant_id is the unique identifier

    // Define the parameters to check if the restaurant already exists
    const getParams = {
        TableName: 'Restaurants',
        Key: { restaurant_id }
    };

    try {
        // Check if the restaurant already exists
        const result = await dynamodb.get(getParams).promise();

        if (result.Item) {
            // Restaurant already exists
            res.status(400).send({ message: 'Restaurant already exists' });
        } else {
            // Restaurant does not exist, add it to the table
            const putParams = {
                TableName: 'Restaurants',
                Item: restaurant // Assumes that the restaurant object has all required attributes
            };

            await dynamodb.put(putParams).promise();

            res.status(201).send({ message: 'Restaurant added successfully' });
        }
    } catch (error) {
        console.error('Error adding restaurant:', error);
        res.status(500).send({ message: 'An error occurred while adding the restaurant' });
    }
});


app.get('/restaurants/:restaurantName', async (req, res) => {
    const AWS = require('aws-sdk');
    const dynamodb = new AWS.DynamoDB.DocumentClient();

    const restaurantName = req.params.restaurantName;

    // Define the parameters for querying the DynamoDB table using the GSI
    const queryParams = {
        TableName: 'Restaurants',
        IndexName: 'RestaurantNameIndex', // The name of the GSI
        KeyConditionExpression: 'restaurant_name = :name',
        ExpressionAttributeValues: {
            ':name': restaurantName
        }
    };

    try {
        // Perform the query operation
        const result = await dynamodb.query(queryParams).promise();

        if (result.Items && result.Items.length > 0) {
            // Restaurant found, return the details
            res.status(200).send(result.Items[0]);
        } else {
            // Restaurant not found
            res.status(404).send({ message: 'Restaurant not found' });
        }
    } catch (error) {
        console.error('Error retrieving restaurant:', error);
         res.status(500).send({ message: 'An error occurred while retrieving the restaurant' });
    }
});


app.delete('/restaurants/:restaurantName', async (req, res) => {
    const AWS = require('aws-sdk');
    const dynamodb = new AWS.DynamoDB.DocumentClient();

    const restaurantName = req.params.restaurantName;

    // Define the parameters for querying the DynamoDB table using the GSI
    const queryParams = {
        TableName: 'Restaurants',
        IndexName: 'RestaurantNameIndex', // The name of the GSI
        KeyConditionExpression: 'restaurant_name = :name',
        ExpressionAttributeValues: {
            ':name': restaurantName
        }
    };

    try {
        // Perform the query operation to find the restaurant by name
        const result = await dynamodb.query(queryParams).promise();

        if (result.Items && result.Items.length > 0) {
            const restaurantId = result.Items[0].restaurant_id;

            // Define the parameters for deleting the item from the table
            const deleteParams = {
                TableName: 'Restaurants',
                Key: {
                    restaurant_id: restaurantId
                }
            };

            // Perform the delete operation
            await dynamodb.delete(deleteParams).promise();

            // Return success message
            res.status(200).send({ message: 'Restaurant deleted successfully' });
        } else {
            // Restaurant not found
            res.status(404).send({ message: 'No such restaurant exists to delete' });
        }
    } catch (error) {
        console.error('Error deleting restaurant:', error);
        res.status(500).send({ message: 'An error occurred while deleting the restaurant' });
    }
});


app.post('/restaurants/rating', async (req, res) => {
    const AWS = require('aws-sdk');
    const dynamodb = new AWS.DynamoDB.DocumentClient();

    const restaurantName = req.body.name;
    const newRating = req.body.rating;

    // Define the parameters for querying the DynamoDB table using the GSI
    const queryParams = {
        TableName: 'Restaurants',
        IndexName: 'RestaurantNameIndex', // The name of the GSI
        KeyConditionExpression: 'restaurant_name = :name',
        ExpressionAttributeValues: {
            ':name': restaurantName
        }
    };

    try {
        // Perform the query operation to find the restaurant by name
        const result = await dynamodb.query(queryParams).promise();

        if (result.Items && result.Items.length > 0) {
            const restaurant = result.Items[0];
            const restaurantId = restaurant.restaurant_id;

            // Calculate the new average rating
            const currentRating = restaurant.rating || 0;
            const ratingCount = restaurant.rating_count || 0;

            const newRatingCount = ratingCount + 1;
            const newAverageRating = ((currentRating * ratingCount) + newRating) / newRatingCount;

            // Define the parameters for updating the item in the table
            const updateParams = {
                TableName: 'Restaurants',
                Key: {
                    restaurant_id: restaurantId
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
            res.status(200).send({ message: 'Rating added successfully', newAverageRating });
        } else {
            // Restaurant not found
            res.status(404).send({ message: 'Restaurant not found' });
        }
    } catch (error) {
        console.error('Error adding rating:', error);
        res.status(500).send({ message: 'An error occurred while adding the rating' });
    }
});


app.get('/restaurants/cuisine/:cuisine', async (req, res) => {
    const AWS = require('aws-sdk');
    const dynamodb = new AWS.DynamoDB.DocumentClient();

    const cuisine = req.params.cuisine;
    let limit = parseInt(req.query.limit, 10) || 10;

    // Ensure the limit is within the acceptable range
    if (limit > 100) {
        limit = 100;
    }

    // Define the parameters for querying the DynamoDB table using the GSI
    const queryParams = {
        TableName: 'Restaurants',
        IndexName: 'CuisineRatingIndex', // The name of the GSI (assumes GSI on cuisine and rating)
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

        if (result.Items && result.Items.length > 0) {
            // Return the list of top-rated restaurants
            res.status(200).send(result.Items);
        } else {
            // No restaurants found for the given cuisine
            res.status(404).send({ message: 'No restaurants found for this cuisine' });
        }
    } catch (error) {
        console.error('Error retrieving top-rated restaurants:', error);
        res.status(500).send({ message: 'An error occurred while retrieving top-rated restaurants' });
    }
});


app.get('/restaurants/region/:region', async (req, res) => {
    const AWS = require('aws-sdk');
    const dynamodb = new AWS.DynamoDB.DocumentClient();

    const region = req.params.region;
    let limit = parseInt(req.query.limit, 10) || 10;

    // Ensure the limit is within the acceptable range
    if (limit > 100) {
        limit = 100;
    }

    // Define the parameters for querying the DynamoDB table using the GSI
    const queryParams = {
        TableName: 'Restaurants',
        IndexName: 'RegionRatingIndex', // The name of the GSI (assumes GSI on region and rating)
        KeyConditionExpression: 'region = :region',
        ExpressionAttributeValues: {
            ':region': region
        },
        ScanIndexForward: false, // This ensures that results are sorted in descending order by rating
        Limit: limit
    };

    try {
        // Perform the query operation to get the top-rated restaurants by region
        const result = await dynamodb.query(queryParams).promise();

        if (result.Items && result.Items.length > 0) {
            // Return the list of top-rated restaurants
            res.status(200).send(result.Items);
        } else {
            // No restaurants found for the given region
            res.status(404).send({ message: 'No restaurants found for this region' });
        }
    } catch (error) {
        console.error('Error retrieving top-rated restaurants:', error);
        res.status(500).send({ message: 'An error occurred while retrieving top-rated restaurants' });
    }
});

app.get('/restaurants/region/:region/cuisine/:cuisine', async (req, res) => {
    const region = req.params.region;
    const cuisine = req.params.cuisine;

    // Students TODO: Implement the logic to get top rated restaurants by region and cuisine
    res.status(404).send("need to implement");
});

app.listen(80, () => {
    console.log('Server is running on http://localhost:80');
});

module.exports = { app };