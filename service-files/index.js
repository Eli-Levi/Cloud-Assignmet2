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
    const restaurant = req.body;

    // Students TODO: Implement the logic to add a restaurant
    res.status(404).send("need to implement");
});

app.get('/restaurants/:restaurantName', async (req, res) => {
    const restaurantName = req.params.restaurantName;

    // Students TODO: Implement the logic to get a restaurant by name
    res.status(404).send("need to implement");
});

app.delete('/restaurants/:restaurantName', async (req, res) => {
    const restaurantName = req.params.restaurantName;
    
    // Students TODO: Implement the logic to delete a restaurant by name
    res.status(404).send("need to implement");
});

app.post('/restaurants/rating', async (req, res) => {
    const restaurantName = req.body.name;
    const rating = req.body.rating;
    
    // Students TODO: Implement the logic to add a rating to a restaurant
    res.status(404).send("need to implement");
});

app.get('/restaurants/cuisine/:cuisine', async (req, res) => {
    const cuisine = req.params.cuisine;
    let limit = req.query.limit;
    
    // Students TODO: Implement the logic to get top rated restaurants by cuisine
    res.status(404).send("need to implement");
});

app.get('/restaurants/region/:region', async (req, res) => {
    const region = req.params.region;
    let limit = req.query.limit;
    
    // Students TODO: Implement the logic to get top rated restaurants by region
    res.status(404).send("need to implement");
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