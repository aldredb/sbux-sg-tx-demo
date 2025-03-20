// optimized-fastify-server.js
const fastify = require('fastify')({ 
  logger: true
});
const { MongoClient, MongoError } = require('mongodb');
const { faker } = require('@faker-js/faker');

// MongoDB connection - Single global instance for optimal connection reuse
let mongoClient = null;
let isConnecting = false;
const mongoConnectPromises = [];

// MongoDB connection options for optimal pooling
const MONGO_OPTIONS = {
  maxPoolSize: 100,       // Maximum number of connections in the pool
  minPoolSize: 10,        // Minimum number of connections to maintain
  maxIdleTimeMS: 30000,   // How long a connection can remain idle before being removed
  connectTimeoutMS: 5000, // Connection timeout
  socketTimeoutMS: 45000, // Socket timeout
  waitQueueTimeoutMS: 10000, // How long operations wait for a connection
  retryWrites: true,
  retryReads: true,
  w: 'majority'
};

// Helper function to sleep
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Singleton pattern for MongoDB connection
async function getMongoClient() {
  // If we already have a client, return it
  if (mongoClient) {
    return mongoClient;
  }
  
  // If a connection is in progress, wait for it
  if (isConnecting) {
    return new Promise((resolve) => {
      mongoConnectPromises.push(resolve);
    });
  }
  
  // Start a new connection
  isConnecting = true;
  
  try {
    const uri = process.env.CLUSTER2;
    if (!uri) {
      throw new Error('MongoDB connection string not provided in environment variables');
    }
    
    mongoClient = new MongoClient(uri, MONGO_OPTIONS);
    
    // Add connection monitoring events
    mongoClient.on('connectionPoolCreated', (event) => {
      fastify.log.info('MongoDB connection pool created');
    });
    
    mongoClient.on('connectionPoolClosed', (event) => {
      fastify.log.info('MongoDB connection pool closed');
    });
    
    // Connect to the MongoDB server
    await mongoClient.connect();
    fastify.log.info('Successfully connected to MongoDB');
    
    // Test the connection by pinging
    await mongoClient.db('admin').command({ ping: 1 });
    fastify.log.info('MongoDB connection verified with ping');
    
    // Resolve any pending connection promises
    mongoConnectPromises.forEach(resolve => resolve(mongoClient));
    
    return mongoClient;
  } catch (error) {
    fastify.log.error('Failed to connect to MongoDB:', error);
    mongoClient = null;
    throw error;
  } finally {
    isConnecting = false;
  }
}

// Execute a transaction with retry logic
async function executeTransactionWithRetry(walletId, amountToDeduct, maxRetries, retryDelaySec, sleepDuration) {
  const client = await getMongoClient();
  const db = client.db("starbucks");
  const walletsCollection = db.collection("wallets");
  const ordersCollection = db.collection("orders");
  
  let retryCount = 0;
  
  while (true) {
    const clientSession = await client.startSession();
    clientSession.startTransaction();
    
    try {
      // Deduct the amount from the wallet balance
      const walletUpdateResult = await walletsCollection.updateOne(
        { _id: walletId, balance: { $gte: amountToDeduct } },
        { $inc: { balance: -1 * amountToDeduct }},
        { session: clientSession }
      );

      // Check if balance is sufficient
      if (walletUpdateResult.matchedCount === 0) {
        await clientSession.abortTransaction();
        await clientSession.endSession();
        return { success: false, message: "Insufficient balance", retryCount };
      }

      // Create an order document with the payment method. 
      // In reality, the order is created first. But for sake of simplicity, we create it here with upsert: true
      const orderId = `O${faker.number.int({ min: 1, max: 5000000 })}`;
      await ordersCollection.updateOne(
        { _id: orderId, },
        {
          $set: {
            payment: {
              method: "wallet", walletId: walletId, amount: amountToDeduct, txDate: new Date()
            }
           },
        },
        { upsert: true, session: clientSession }
      )

      if (sleepDuration > 0) {
        await sleep(sleepDuration);
      }

      await clientSession.commitTransaction();
      await clientSession.endSession();
      return { success: true, message: "Transaction successful", retryCount };
      
    } catch (error) {
      if (error instanceof MongoError && error.hasErrorLabel('TransientTransactionError')) {
        retryCount++;
        if (retryCount <= maxRetries) {
          fastify.log.info(`TransientTransactionError for ${walletId}. Retry ${retryCount}/${maxRetries}`);
          await clientSession.abortTransaction();
          await clientSession.endSession();
          await sleep(retryDelaySec * 1000);
          continue; // Retry the transaction
        } else {
          fastify.log.info(`Max retries (${maxRetries}) reached for ${walletId}`);
        }
      }
      
      await clientSession.abortTransaction();
      await clientSession.endSession();
      return { 
        success: false, 
        message: "Transaction failed", 
        error: error.message,
        retryCount
      };
    }
  }
}

// Define schema for better validation and performance
const transactionSchema = {
  schema: {
    body: {
      type: 'object',
      required: ['walletId', 'amount'],
      properties: {
        walletId: { type: 'string' },
        amount: { type: 'number' },
        maxRetries: { type: 'number', default: 3 },
        retryDelay: { type: 'number', default: 2 },
        sleep: { type: 'number', default: 0 },
        testId: { type: 'string' }
      }
    },
    response: {
      200: {
        type: 'object',
        properties: {
          success: { type: 'boolean' },
          message: { type: 'string' },
          retryCount: { type: 'number' },
          testId: { type: 'string' },
          error: { type: 'string' }
        }
      }
    }
  }
};

// Register routes
fastify.post('/execute-transaction', transactionSchema, async (request, reply) => {
  const { walletId, amount, maxRetries = 3, retryDelay = 2, sleep = 0, testId } = request.body;
  
  try {
    // Execute the transaction with retry logic
    const result = await executeTransactionWithRetry(
      walletId, 
      amount, 
      maxRetries, 
      retryDelay, 
      sleep
    );
    
    // Add test metadata
    if (testId) {
      result.testId = testId;
    }
    
    return result;
  } catch (error) {
    fastify.log.error('Error executing transaction:', error);
    return reply.code(500).send({ 
      success: false, 
      message: "Server error", 
      error: error.message 
    });
  }
});

// Health check endpoint
fastify.get('/health', async () => {
  try {
    // Check MongoDB connection
    const client = await getMongoClient();
    await client.db('admin').command({ ping: 1 });
    return { status: 'ok', mongodb: 'connected' };
  } catch (error) {
    return { status: 'error', mongodb: 'disconnected', error: error.message };
  }
});

// Register graceful shutdown for proper cleanup
function gracefulShutdown(signal) {
  fastify.log.info(`Received ${signal}, starting graceful shutdown...`);
  
  fastify.close(async () => {
    fastify.log.info('Fastify server closed');
    
    if (mongoClient) {
      try {
        await mongoClient.close();
        fastify.log.info('MongoDB connection closed');
      } catch (err) {
        fastify.log.error('Error closing MongoDB connection:', err);
      }
    }
    
    process.exit(0);
  });
}

// Listen for shutdown signals
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Start the server
const start = async () => {
  try {
    // Connect to MongoDB before starting the server
    await getMongoClient();
    
    // Set server timeouts for long-running transactions
    fastify.server.keepAliveTimeout = 120000;
    fastify.server.headersTimeout = 121000;
    
    // Start listening
    await fastify.listen({ port: process.env.PORT || 3001, host: '0.0.0.0' });
    fastify.log.info(`Server listening on ${fastify.server.address().port}`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();