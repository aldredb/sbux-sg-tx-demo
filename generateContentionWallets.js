const { MongoClient } = require('mongodb');
const { faker } = require('@faker-js/faker');

// MongoDB connection string - update with your actual connection details
const uri = process.env.CLUSTER2;
if (!uri) {
    throw new Error('MongoDB connection string not provided in environment variables');
}

const dbName = 'starbucks';
const collectionName = 'wallets';

// Number of contention test wallet documents to create
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

// Function to generate a contention test wallet document with SW prefix
function generateContentionWallet(index) {
  return {
    _id: `SW${index + 1}`,
    customer_ids: generateCustomerIds(),
    balance: 500, // Lower balance for contention test wallets
    contentionTest: true
  };
}

// Main function to connect to MongoDB and insert contention test wallet documents
async function insertContentionWallets() {
  const client = new MongoClient(uri);
  
  try {
    console.log('Connecting to MongoDB...');
    await client.connect();
    console.log('Connected successfully to MongoDB');
    
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    
    // First delete any existing contention test wallets (both by prefix and flag)
    try {
      // Delete by contentionTest flag
      const deleteFlagResult = await collection.deleteMany({ contentionTest: true });
      console.log(`Deleted ${deleteFlagResult.deletedCount} existing wallets with contentionTest=true`);
    } catch (error) {
      console.log('Error deleting existing contention test wallets:', error);
    }
    
    // Create partial index on contentionTest field if it doesn't exist
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
    
    // Insert contention test wallets
    console.log(`Starting to insert ${CONTENTION_WALLET_COUNT} contention test wallet documents...`);
    
    const contentionBatch = [];
    for (let i = 0; i < CONTENTION_WALLET_COUNT; i++) {
      contentionBatch.push(generateContentionWallet(i));
    }
    
    await collection.insertMany(contentionBatch);
    console.log(`${CONTENTION_WALLET_COUNT} contention test wallets inserted`);
    
    // Verify the count
    const contentionCount = await collection.countDocuments({ contentionTest: true });
    console.log(`Contention test wallets: ${contentionCount}`);
    
  } catch (error) {
    console.error('Error during contention wallet insertion:', error);
  } finally {
    await client.close();
    console.log('MongoDB connection closed');
  }
}

// Run the insertion process
insertContentionWallets()
  .then(() => console.log('Script execution completed'))
  .catch(err => console.error('Script failed:', err));