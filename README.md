# Assignment 2

# The Assignment Story
We are going to build an API for "Wolt" Semi like platform. 
Our main entity is Restaurants, and we are going to build backend platform for this app. 
Each Restaurant has a **Unique name**, **Regional Geo Location** that operate there (For example, North, Tel Aviv, Ariel, Central District, etc..), **Raiting** in the system base on customer feedback (Between 1 to 5, can be fractions for example, 4.8), and **Cuisine** (for example Bisto, Pub, Cafe, etc..). 

For this Assigment we will build just the Backend API and no need to implement Frontend (client side). 
We will focus about how we should structure the table in *Dynamo DB* for effient access pattern, and we will accelerate our calls with *Memcached* so for the havvy hitter in our system we will not access to the DB so frequency. 

Our Stack is:
 - DynamoDB for persistense storage.
 - Memcached on EC2 / Memcached on ElasticCache for Cache.
 - EC2 + Load balancer + Auto Scaling group for running the application. 
 - CDK For Intrastructure provisioning and Deployment. 
 - S3 for Storing artifact files (service files). 

# Prerequisite
> We will do this task on Github Codespace and not with the good old **Workspace** in order to save some AWS Credits for the final project. 
1. Create a repo from this template repository
2. Create a [Github Codespace](https://github.com/features/codespaces) for this repository
3. Open codespace in your vscode

### Prepare your Codespace for development
#### Install AWS CLI
```bash
# Download the AWS CLI 
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" 
# Unzip the package
unzip awscliv2.zip 
# Install the AWS CLI
sudo ./aws/install 
# Clean up the installation files 
rm -rf awscliv2.zip aws
``` 
#### Set your Credentials

Inside `Launch AWS Academy Learner Lab` section go to `AWS Details`
and then click on `show` close to the `AWS CLI`.
copy the credentials.
and write `nano ~/.aws/credentials`
then paste the credentials.
for verification, just write`aws s3 ls` 
and make sure that you see some s3 bucket from you account. 

#### Install AWS CDK 
```bash
# Install Typescript
npm -g install typescript

# Install CDK
npm install -g aws-cdk

# Verify that CDK Is installed
cdk --version
```
### Bootstrap your account for CDK

> Note that: If you already bootstrap your account, no need to execute that action
```bash
# Go to CDK Directory
cd restaurants-cdk

# Install NPM models
npm install

# Run bootsrap
cdk bootstrap --template bootstrap-template.yaml
```

### Deploy the Base Stack
Make sure that you are at folder `restaurants-cdk`
Change the Account ID and the VPC ID for your own details,
you can find all the places easily when searching `Students TODO Account Details`
```bash
cdk deploy
```
The output from the deploy should looks like
```bash
 ✅  RestaurantsCdkStack

✨  Deployment time: 80.27s

Outputs:
RestaurantsCdkStack.BucketName = restaurantscdkstack-deploymentartifactc01e7257-bx6hao6vyzsr
RestaurantsCdkStack.LoadBalancerURL = http://Restau-LB8A1-0nYWT3QCaC8I-1300768425.us-east-1.elb.amazonaws.com
RestaurantsCdkStack.MemcachedInstanceEndpoint = ec2-34-228-69-251.compute-1.amazonaws.com:11211
RestaurantsCdkStack.MemcachedInstancePublicIp = 34.228.69.251
RestaurantsCdkStack.RunTestCommand = MEMCACHED_CONFIGURATION_ENDPOINT='ec2-34-228-69-251.compute-1.amazonaws.com:11211' TABLE_NAME='RestaurantsCdkStack-RestaurantsE94BF231-QOIXONJURAS5' AWS_REGION='us-east-1' USE_CACHE='true' npm test
RestaurantsCdkStack.TableName = RestaurantsCdkStack-RestaurantsE94BF231-QOIXONJURAS5
Stack ARN:
arn:aws:cloudformation:us-east-1:079553702230:stack/RestaurantsCdkStack/04eef370-2bd2-11ef-9d4e-0ee2607800ef

✨  Total time: 87.4s
```
Now just copy the LB URL, open it in the browser, and you should get something like this:
```json
{
  "MEMCACHED_CONFIGURATION_ENDPOINT": "ec2-34-224-156-184.compute-1.amazonaws.com:11211",
  "TABLE_NAME": "RestaurantsCdkStack-RestaurantsE94BF231-QOIXONJURAS5",
  "AWS_REGION": "us-east-1",
  "USE_CACHE": true
}
```
Now, after we have a good baseline, this is the time for the actual work 😊

# What we have till now
- DynamoDB Table with basic structure
- EC2 Instance that running Memcached with Public IP
- Application running and connected to LB with basic express application running on, include a enviorment variable that contains the connection details for the DynamoDB Table and Memcached configuration endpoint.

All of this is already created for you, with the CDK code and Service files that we already provided.

## Project Structure
### CDK Files Structure
```
restaurants-cdk/
├── bin/ 
│ └── restaurants-cdk.ts 
├── lib/ 
│ └── restaurants-cdk-stack.ts
├── package.json 
├── package-lock.json 
├── cdk.json 
└── tsconfig.json
```
restaurants-cdk is mainly defined the infrasturture for you application, read carfully all the code inside `restaurants-cdk-stack.ts` in order to understand we already doing for you. 

### Service Files Structure
```
service-files/
├── model/ 
│ |── restaurantsMemcachedActions.js
│ └── your-additional-files-for-access-dynamo-db-etc..
├── index.js 
├── index.test.js 
├── package.json 
├── package-lock.json 
└── tsconfig.json
```
Inside `index.js` you can find all the base API that needs to implements in this assigments
most of them return 404 since you need to implement them. 

Inside `index.test.js` you can find basic test for that API that already written for you (we will describe later how to run them). 

Inside `restaurantsMemcachedActions.js` you can find ready to use code for store JSON Data inside Memcached, it will help you in the next steps for this assigment. 

## Understaning the API
### Restaurants API Endpoints

#### Get API Configuration

##### GET /
Returns the current configuration settings of the API.

#### Add a New Restaurant

##### POST /restaurants
Adds a new restaurant to the database. Returns a success message or error if the restaurant already exists.

#### Get a Restaurant by Name

##### GET /restaurants/:restaurantName
Retrieves details of a restaurant by its name. Returns 404 if not found.

#### Delete a Restaurant by Name

##### DELETE /restaurants/:restaurantName
Deletes a restaurant by its name. Returns a success message.

#### Add a Rating to a Restaurant

##### POST /restaurants/rating
Adds a rating to a restaurant and calculates the average rating. Returns a success message.

#### Get Top Rated Restaurants by Cuisine

##### GET /restaurants/cuisine/:cuisine
Retrieves top-rated restaurants by cuisine. Supports an optional `limit` query parameter (default 10, max 100).

#### Get Top Rated Restaurants by Region

##### GET /restaurants/region/:region
Retrieves top-rated restaurants by region. Supports an optional `limit` query parameter (default 10, max 100).

#### Get Top Rated Restaurants by Region and Cuisine

##### GET /restaurants/region/:region/cuisine/:cuisine
Retrieves top-rated restaurants by region and cuisine. Supports an optional `limit` query parameter (default 10, max 100).

### Run The tests
There is **basic tests** that prepered in order to help you for basic check for your API Correctness. 
before we start, try run that test. 

In the deployment CDK output, you can find `RunTestCommand`
just copy paste the command in order to run the tests. 
make sure that you are inside folder `cd /workspaces/ariel-cloud-assigment-2/service-files`

The end of the test result should looks like:

```j
Test Suites: 1 failed, 1 total
Tests:       12 failed, 1 passed, 13 total
Snapshots:   0 total
Time:        0.539 s
Ran all test suites.
```
The only test that is passed is the API For Endpoint configuration overview, that you can find in case of access the the Load balancer URL. 

> After you will completely implemented the Basic API (without cache) all the test should be passed successfuly. 

## Tasks

### Implement the API with DynamoDB
In this phase, you need to design your table base on the API needed,
modify the `createDynamoDBTable` , change the Partition key and Range key as needed, and added the indexes that you need. 
after that, deploy the stack with `cdk deploy`
and continue to modify your code in the service file, so the API will return the value in efficient way.
> RunTestCommand may change after the deployment, make sure that you copy it every time

All the operation should be "eventually consistant", at any given time the data should be valid.
Try thinking how to do optimization on the "Dynamo way" for some of the operations. 

#### 10 points bonus
add to the api of ```GET /restaurants/cuisine/:cuisine``` ability to get restaurants with Raiting bigger than x. 
#### Good to know 
- `Region` is a reserved kewords in DynamoDB, so better to use `GeoRegion` 

#### Run the tests 
At this phase, the base test should run and be "green" constantly. 
**You shold not change any test in order to make it passeed**
feel free to add additional tests, it will probably help you in the next phases.

thease tests will help you to validate that you are not breacking anything while you are running optimization on your featching and calculation procccess. 

### Add Cache to Your Application

Now it's the time for adding cache to your application,
use the additional class that we provide `restaurantsMemcachedActions.js` this will help you to access memcached with key-value pattern (like hash map!)

consider how you can define the key so it will help you do cache multiple items/single items. 
for example, single Restaurant can be store in the cache with the key `Restaurant-<Restaurant-Name>`. 

For effieient cache, we will accept that sometimes the data that we will retrun will not be the most update one, but it must be consistant in any phase. 
we will keep the eventually consistancy, but we will always keep it with delay of maximum *t* Time, for example 10 seconds. 

Another requirement is that in case of one of the operations as part of some request handling is tauching the db, you must return the most updated data. 

In the code we have this flag defined `const  USE_CACHE  =  process.env.USE_CACHE  ===  'true';` you can define it from the SDK, while you implement the cache, you still need to preserved the behavior without cache, that will be control by the flag (can changes from CDK). 

> In this phase, the test will become inconsistant, for example, for  some time, it can fail, and sometimes pass. 
> but after 10 seconds (or t time) in should always pass. 
> try thinking how you can added tests that validation that flows if you want. 

### Implement External (Integration) Tests for the App
now it's a good time to deploy the app and make sure that everything  works in AWS deployment. 
after that, you can switch to the Elastic Cache instead of the single Memcache instance that we spin. 
simple change the code to be like that:
```typescript
// const  memcachedConfigurationEndpoint  =  this.createMemcachedSingleInstaceInPublicSubnetForTestingPurpose(vpc, labRole);

// Students TODO: Comment out the above line and uncomment the below line to use Elasticache, for the testing phase

const memcachedConfigurationEndpoint = this.createMemcachedElasticache(vpc, labRole);
```

You can modify inside the function how many instances you need for you cache. usually 1 will be okay, but in case you want to stress extreamly the system you can spin more instances. 

Now write testing script, as we done at Assigment 1 for load test the system, with cache enabled, and without. 
record the result in some file that you will attatch to the submission. 

# Good luck! 