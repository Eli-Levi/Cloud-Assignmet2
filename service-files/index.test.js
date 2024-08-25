const request = require('supertest');
const { app } = require('./index');

let server;

const RestaurantAName = 'Restaurant A' + Date.now();

beforeAll((done) => {
  server = app.listen(3000, () => {
      done();
  });
});

afterAll((done) => {
  server.close(() => {
    done()
  });
});

describe('GET /', () => {
  it('should return the environment variables', async () => {
    const response = await request(server).get('/');
    expect(response.status).toBe(200);
    expect(response.body).toEqual({
      MEMCACHED_CONFIGURATION_ENDPOINT: process.env.MEMCACHED_CONFIGURATION_ENDPOINT,
      TABLE_NAME: process.env.TABLE_NAME,
      AWS_REGION: process.env.AWS_REGION,
      USE_CACHE: process.env.USE_CACHE === 'true'
    });
  });
});

describe('POST /restaurants', () => {
  it('should add a restaurant', async () => {
    const restaurant = { name: RestaurantAName, cuisine: 'Italian', region: 'North'};
    const response = await request(server).post('/restaurants').send(restaurant);
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ success: true });
  });

  it('should fail to create the same restaurant', async () => {
    const restaurant = { name: RestaurantAName, cuisine: 'Italian', region: 'North'};
    const response = await request(server).post('/restaurants').send(restaurant);
    expect(response.status).toBe(409);
    expect(response.body).toEqual({ success: false , message: 'Restaurant already exists' });
  });
});

describe('GET /restaurants/:restaurantName', () => {
  it('should get a restaurant by name', async () => {
    const restaurantName = RestaurantAName;
    const response = await request(server).get(`/restaurants/${restaurantName}`);
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ name: RestaurantAName, cuisine: 'Italian', rating: 0, region: 'North' });
  });
});

describe('POST /restaurants/rating', () => {
  it('should add a rating to a restaurant', async () => {
    const rating = { name: RestaurantAName, rating: 5 };
    const response = await request(server).post('/restaurants/rating').send(rating);
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ success: true });
  });

  it('should get a restaurant by name with rating', async () => {
    const restaurantName = RestaurantAName;
    const response = await request(server).get(`/restaurants/${restaurantName}`);
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ name: RestaurantAName, cuisine: 'Italian', rating: 5, region: 'North' });
  });

  it('should add another rating to a restaurant', async () => {
    const rating = { name: RestaurantAName, rating: 4 };
    const response = await request(server).post('/restaurants/rating').send(rating);
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ success: true });
  });

  it('should get a restaurant by name with updated rating', async () => {
    const restaurantName = RestaurantAName;
    const response = await request(server).get(`/restaurants/${restaurantName}`);
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ name: RestaurantAName, cuisine: 'Italian', rating: 4.5, region: 'North' });
  });
});

describe('GET /restaurants/cuisine/:cuisine', () => {
  it('should get top restaurants by cuisine', async () => {
    const cuisine = 'Italian';
    const response = await request(server).get(`/restaurants/cuisine/${cuisine}`);
    expect(response.status).toBe(200);
    expect(response.body).toContainEqual({ name: RestaurantAName, cuisine: 'Italian', rating: 4.5, region: 'North' });
  });
});

describe('GET /restaurants/region/:region', () => {
  it('should get top restaurants by region', async () => {
    const region = 'North';
    const response = await request(server).get(`/restaurants/region/${region}`);
    expect(response.status).toBe(200);
    expect(response.body).toContainEqual({ name: RestaurantAName, cuisine: 'Italian', rating: 4.5, region: 'North' });
  });
});

describe('GET /restaurants/region/:region/cuisine/:cuisine', () => {
  it('should get top restaurants by region and cuisine', async () => {
    const region = 'North';
    const cuisine = 'Italian';
    const response = await request(server).get(`/restaurants/region/${region}/cuisine/${cuisine}`);
    expect(response.status).toBe(200);
    expect(response.body).toContainEqual({ name: RestaurantAName, cuisine: 'Italian', rating: 4.5, region: 'North' });
  });
});

describe('DELETE /restaurants/:restaurantName', () => {
  it('should delete a restaurant by name', async () => {
    const restaurantName = RestaurantAName;
    const response = await request(server).delete(`/restaurants/${restaurantName}`);
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ success: true });
  });

  it('should not get a restaurant by name', async () => {
    const restaurantName = RestaurantAName;
    const response = await request(server).get(`/restaurants/${restaurantName}`);
    expect(response.status).toBe(404);
  });
});