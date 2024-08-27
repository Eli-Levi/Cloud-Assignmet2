const http = require('http');
const assert = require('assert');

const endPoint = 'Restau-LB8A1-072zLk5EliCA-690384357.us-east-1.elb.amazonaws.com';
const port = 80;

const restaurantName = 'anExampleRestaurant';

const cuisineNames = [
    "cuisine0", "cuisine1", "cuisine2", "cuisine3", "cuisine4", "cuisine5",
    "cuisine6", "cuisine7", "cuisine8", "cuisine9", "cuisine10", "cuisine11",
    "cuisine12", "cuisine13", "cuisine14", "cuisine15", "cuisine16", "cuisine17",
    "cuisine18", "cuisine19", "cuisine20"
];

const regionNames = [
    "city0", "city1", "city2", "city3", "city4", "city5", "city6", "city7",
    "city8", "city9", "city10", "city11", "city12", "city13", "city14", "city15",
    "city16", "city17", "city18", "city19", "city20"
];

const numRequests = 1000000;

const makeRequest = (options, postData = null) => {
    return new Promise((resolve, reject) => {
        const req = http.request(options, (res) => {
            let data = '';
            res.on('data', (chunk) => { data += chunk; });
            res.on('end', () => { resolve({ statusCode: res.statusCode, data }); });
        });

        req.on('error', (e) => { reject(e); });

        if (postData) { req.write(postData); }
        req.end();
    });
};

const testPostMethod = async (i) => {
    const restaurant = {
        name: `${restaurantName}${i}`,
        cuisine: cuisineNames[i % cuisineNames.length],
        region: regionNames[i % regionNames.length]
    };

    const options = {
        hostname: endPoint,
        port: port,
        path: '/restaurants',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
    };

    try {
        const startTime = process.hrtime();
        const response = await makeRequest(options, JSON.stringify(restaurant));
        const endTime = process.hrtime(startTime);
        const elapsedTimeMs = ((endTime[0] * 1e9 + endTime[1]) / 1e6).toFixed(2);

        assert.strictEqual(response.statusCode, 200, 'POST status code should be 200');

        console.log(`POST ${options.path} Status Code: ${response.statusCode}; Time Elapsed: ${elapsedTimeMs}ms`);
    } catch (error) {
        console.error('POST Test failed:', error);
    }
};

const testGetMethod = async (i) => {
    const options = {
        hostname: endPoint,
        port: port,
        path: `/restaurants/${restaurantName}${i}`,
        method: 'GET'
    };

    try {
        const startTime = process.hrtime();
        const response = await makeRequest(options);
        const endTime = process.hrtime(startTime);
        const elapsedTimeMs = ((endTime[0] * 1e9 + endTime[1]) / 1e6).toFixed(2);

        assert.strictEqual(response.statusCode, 200, 'GET status code should be 200');
        const data = JSON.parse(response.data);
        assert.strictEqual(data.name, `${restaurantName}${i}`, 'Restaurant name mismatch');
        assert.strictEqual(data.cuisine, cuisineNames[i % cuisineNames.length], 'Cuisine mismatch');
        assert.strictEqual(data.region, regionNames[i % regionNames.length], 'Region mismatch');

        console.log(`GET ${options.path} Status Code: ${response.statusCode}; Time Elapsed: ${elapsedTimeMs}ms`);
    } catch (error) {
        console.error('GET Test failed:', error);
    }
};

const testDeleteMethod = async (i) => {
    const restaurantNameToDelete = `${restaurantName}${i}`;

    const options = {
        hostname: endPoint,
        port: port,
        path: `/restaurants/${restaurantNameToDelete}`,
        method: 'DELETE'
    };

    try {
        const startTime = process.hrtime();
        const response = await makeRequest(options);
        const endTime = process.hrtime(startTime);
        const elapsedTimeMs = ((endTime[0] * 1e9 + endTime[1]) / 1e6).toFixed(2);

        assert.strictEqual(response.statusCode, 200, 'DELETE status code should be 200');
        const data = JSON.parse(response.data);
        assert.deepStrictEqual(data, { success: true }, 'Expected success message');

        console.log(`DELETE ${options.path} Status Code: ${response.statusCode}; Time Elapsed: ${elapsedTimeMs}ms`);
    } catch (error) {
        console.error('DELETE Test failed:', error);
    }
};

const loadTest = async () => {
    console.log(`Starting load test with ${numRequests} requests`);

    console.log('[+] Testing POST method');
    for (let i = 1; i <= numRequests; i++) {
        await testPostMethod(i);
    }

    console.log('[+] Testing GET methods');
    for (let i = 1; i <= numRequests; i++) {
        await testGetMethod(i);
    }

    console.log('[+] Testing DELETE method');
    for (let i = 1; i <= numRequests; i++) {
        await testDeleteMethod(i);
    }
};

loadTest().catch(console.error);
