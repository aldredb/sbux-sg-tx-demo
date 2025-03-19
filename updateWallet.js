const { MongoClient, MongoError } = require('mongodb');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// Parse command line arguments
const argv = yargs(hideBin(process.argv))
  .option('sleep', {
    alias: 's',
    description: 'Sleep duration in milliseconds before committing',
    type: 'number',
    default: 0
  })
  .option('walletId', {
    alias: 'w',
    description: 'ID of the wallet to deduct from',
    type: 'string',
    default: 'WALLET001'
  })
  .option('amount', {
    alias: 'a',
    description: 'Amount to deduct from the wallet',
    type: 'number',
    default: 50
  })
  .option('maxRetries', {
    alias: 'r',
    description: 'Maximum number of transaction retry attempts',
    type: 'number',
    default: 2
  })
  .option('retryDelay', {
    alias: 'd',
    description: 'Delay between retry attempts in seconds',
    type: 'number',
    default: 5
  })
  .help()
  .alias('help', 'h')
  .argv;

// Helper function to sleep for a specified time in milliseconds
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const sleepDuration = argv.sleep;
const walletId = argv.walletId;
const amountToDeduct = argv.amount;
const maxRetries = argv.maxRetries;
const retryDelaySec = argv.retryDelay;

const mongoClient = new MongoClient(process.env.CLUSTER2);

const db = mongoClient.db("starbucks");
const walletsCollection = db.collection("wallets");

// Function to execute the transaction with retry logic
async function executeTransactionWithRetry() {
  let retryCount = 0;
  
  while (true) {
    const clientSession = await mongoClient.startSession();
    clientSession.startTransaction();
    
    try {
      const updateResult = await walletsCollection.updateOne(
        { _id: walletId, balance: { $gte: amountToDeduct } },
        { $inc: { balance: -1 * amountToDeduct }},
        { session: clientSession }
      );

      console.log(`Update result: ${JSON.stringify(updateResult)}`);

      if (updateResult.matchedCount === 0) {
        console.log(`updateResult - Insufficient balance for wallet: ${walletId}`);
        throw(`Insufficient balance for wallet: ${walletId}`);
      } else {
        console.log(`updateResult - Sufficient balance for wallet: ${walletId}`);

        // Add sleep before committing the transaction
        console.log(`Sleeping for ${sleepDuration}ms before committing...`);
        await sleep(sleepDuration);
        console.log('Sleep finished, now committing transaction');
      }

      await clientSession.commitTransaction();
      console.log('Transaction committed successfully');
      return true; // Success, exit the retry loop
      
    } catch (error) {
      if (error instanceof MongoError && error.hasErrorLabel('TransientTransactionError')) {
        console.log(`TransientTransactionError encountered: ${error.errmsg}`);
        // Handle transient error with retry logic
        retryCount++;
        if (retryCount <= maxRetries) {
          console.log(`Retry attempt ${retryCount}/${maxRetries} in ${retryDelaySec} seconds...`);
          await clientSession.abortTransaction();
          await sleep(retryDelaySec * 1000); // Convert seconds to milliseconds
          continue; // Retry the transaction
        } else {
          console.log(`Maximum retry attempts (${maxRetries}) reached. Giving up.`);
        }
      } else if (error instanceof MongoError && error.hasErrorLabel('UnknownTransactionCommitResult')) {
        console.log(`${error.errorLabels} - ${error.errmsg}`);
      } else {
        console.log('An error occurred in the transaction, aborting: ' + error);
      }
      
      await clientSession.abortTransaction();
      return false; // Failed, exit the retry loop
    } finally {
      await clientSession.endSession();
    }
  }
}

(async () => {
  try {
    await executeTransactionWithRetry();
  } catch (error) {
    console.error('Unhandled error:', error);
  } finally {
    await mongoClient.close();
    console.log('MongoDB connection closed');
  }
})();