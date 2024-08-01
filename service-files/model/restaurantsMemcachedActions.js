const memcache = require("memcache-client");

const CACHE_LIFETIME_IN_SECONDS = 60;

class RestaurantsMemcachedActions {
    /**
     * 
     * @param {string} server 
     */
    constructor(server) {
        this.client = new memcache.MemcacheClient({server: server});
    }

    async addRestaurants(restaurantAccessKey, objectValue) {
        const key = this.__modifyKeyForMemcached(restaurantAccessKey);
        const value = JSON.stringify(objectValue);
        await this.client.set(key, value, {lifetime: CACHE_LIFETIME_IN_SECONDS});
        return true;
    }

    async deleteRestaurants(restaurantAccessKey) {
        const key = this.__modifyKeyForMemcached(restaurantAccessKey);
        await this.client.delete(key);
        return true;
    }

    async getRestaurants(restaurantAccessKey) {
        const key = this.__modifyKeyForMemcached(restaurantAccessKey);
        const cacheResponse = await this.client.get(key);
        if (cacheResponse) {
            return this.__convertMemcachedResponseToObject(cacheResponse);
        }
        return false;
    }

    __modifyKeyForMemcached(key) {
        return key.replace(" ", "_");
    }

    __convertMemcachedResponseToObject(response) {
        if (response === null) {
            return null;
        }
        console.log("cache :: response", response);
        try {
            return JSON.parse(response.value);
        } catch (err) {
            console.log("cache :: error, " + JSON.stringify(response), err);
            return undefined;
        }
    }
}

module.exports = RestaurantsMemcachedActions;