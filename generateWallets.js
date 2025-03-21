const { MongoClient, Double } = require('mongodb');
const { faker } = require('@faker-js/faker');

// MongoDB connection string - update with your actual connection details
const uri = process.env.CLUSTER2;
if (!uri) {
    throw new Error('MongoDB connection string not provided in environment variables');
}

const dbName = 'starbucks';
const collectionName = 'wallets';

// Configuration for wallet documents
const WALLET_COUNT = 100000;
const CONTENTION_WALLET_COUNT = 100;

// Function to generate a random array of customer IDs
function generateCustomerIds() {
  // Generate between 1 and 3 customer IDs
  const count = faker.number.int({ min: 1, max: 3 });
  const customerIds = [];
  
  // Set to keep track of used IDs to avoid duplicates within a wallet
  const usedIds = new Set();
  
  for (let i = 0; i < count; i++) {
    let customerId;
    do {
      // Generate a random customer ID between C1 and C50000
      const customerNumber = faker.number.int({ min: 1, max: 50000 });
      customerId = `C${customerNumber}`;
    } while (usedIds.has(customerId));
    
    usedIds.add(customerId);
    customerIds.push(customerId);
  }
  
  return customerIds;
}

// Function to generate a regular wallet document
function generateWallet(index) {
  return {
    _id: `W${index + 1}`,
    customer_ids: generateCustomerIds(),
    balance: new Double(1000000) // Huge balance for load testing
  };
}

// Function to generate a contention test wallet document with SW prefix
function generateContentionWallet(index) {
  return {
    _id: `SW${index + 1}`,
    customer_ids: generateCustomerIds(),
    balance: new Double(500), // Lower balance for contention test wallets
    contentionTest: true
  };
}

// Main function to connect to MongoDB and insert both types of wallet documents
async function seedDatabase() {
  const client = new MongoClient(uri);
  
  try {
    console.log('Connecting to MongoDB...');
    await client.connect();
    console.log('Connected successfully to MongoDB');
    
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    
    // Step 1: Set up collection with validation
    try {
      // Drop the collection if it exists
      await collection.drop();
      console.log('Existing collection dropped');
      
      // Create collection with schema validation
      await db.createCollection(collectionName, {
        validator: { 
          "$jsonSchema": {
              "required": [ "balance" ],
              "properties": {
                 "balance": {
                    "bsonType": "double",
                    "description": "must not be less than 0",
                    "minimum": 0,
                 }
              }
          }
        },
        validationAction: "error",
        validationLevel: "strict"
      });
      console.log('Created new collection with validation');
      
    } catch (error) {
      console.log('Could not drop collection, continuing with existing one');
    }
    
    // Step 2: Create partial index for contention test wallets
    const indexes = await collection.listIndexes().toArray();
    const contentionIndexExists = indexes.some(index => 
      index.name === "idx_contentionTest" || 
      (index.key && index.key.contentionTest)
    );
    
    if (!contentionIndexExists) {
      await collection.createIndex(
        { contentionTest: 1 },
        { 
          name: "idx_contentionTest",
          partialFilterExpression: { contentionTest: true }
        }
      );
      console.log('Created partial index on contentionTest field');
    } else {
      console.log('Partial index on contentionTest field already exists');
    }
    
    // Step 3: Insert regular wallets in batches
    console.log(`Starting to insert ${WALLET_COUNT} regular wallet documents...`);
    
    const BATCH_SIZE = 10000;
    let inserted = 0;
    
    for (let i = 0; i < WALLET_COUNT; i += BATCH_SIZE) {
      const batch = [];
      const batchEnd = Math.min(i + BATCH_SIZE, WALLET_COUNT);
      
      for (let j = i; j < batchEnd; j++) {
        batch.push(generateWallet(j));
      }
      
      await collection.insertMany(batch);
      inserted += batch.length;
      console.log(`Progress: ${inserted}/${WALLET_COUNT} regular wallets inserted`);
    }
    
    // Step 4: Insert contention test wallets
    console.log(`Starting to insert ${CONTENTION_WALLET_COUNT} contention test wallet documents...`);
    
    const contentionBatch = [];
    for (let i = 0; i < CONTENTION_WALLET_COUNT; i++) {
      contentionBatch.push(generateContentionWallet(i));
    }
    
    await collection.insertMany(contentionBatch);
    console.log(`${CONTENTION_WALLET_COUNT} contention test wallets inserted`);
    
    // Step 5: Verify the counts
    const totalCount = await collection.countDocuments();
    const contentionCount = await collection.countDocuments({ contentionTest: true });
    const regularCount = totalCount - contentionCount;
    
    console.log('Database seeding completed successfully:');
    console.log(`- Total wallet documents: ${totalCount}`);
    console.log(`- Regular wallet documents: ${regularCount}`);
    console.log(`- Contention test wallet documents: ${contentionCount}`);
    
  } catch (error) {
    console.error('Error during database seeding:', error);
  } finally {
    await client.close();
    console.log('MongoDB connection closed');
  }
}

// Run the seeding process
seedDatabase()
  .then(() => console.log('Script execution completed'))
  .catch(err => console.error('Script failed:', err));