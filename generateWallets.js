const { MongoClient } = require('mongodb');
const { faker } = require('@faker-js/faker');

// MongoDB connection string - update with your actual connection details
const uri = process.env.CLUSTER2;
if (!uri) {
    throw new Error('MongoDB connection string not provided in environment variables');
}

const dbName = 'starbucks';
const collectionName = 'wallets';

// Number of wallet documents to create
const WALLET_COUNT = 100000;

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

// Function to generate a wallet document
function generateWallet(index) {
  return {
    _id: `W${index + 1}`,
    customer_ids: generateCustomerIds(),
    balance: 1000000 // Huge balance so that we can perform load test
  };
}

// Main function to connect to MongoDB and insert wallet documents
async function insertWallets() {
  const client = new MongoClient(uri);
  
  try {
    console.log('Connecting to MongoDB...');
    await client.connect();
    console.log('Connected successfully to MongoDB');
    
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    
    // Drop the collection if it exists
    try {
      await collection.drop();
      console.log('Existing collection dropped');
    } catch (error) {
      console.log('No existing collection to drop, creating new one');
    }
    
    console.log(`Starting to insert ${WALLET_COUNT} wallet documents...`);
    
    // Insert wallets in batches to avoid memory issues
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
      console.log(`Progress: ${inserted}/${WALLET_COUNT} wallets inserted`);
    }
    
    console.log('Wallet insertion completed successfully');
    
    // Verify the count
    const count = await collection.countDocuments();
    console.log(`Total documents in collection: ${count}`);
    
  } catch (error) {
    console.error('Error during wallet insertion:', error);
  } finally {
    await client.close();
    console.log('MongoDB connection closed');
  }
}

// Run the insertion process
insertWallets()
  .then(() => console.log('Script execution completed'))
  .catch(err => console.error('Script failed:', err));