// mongodb-wallet-contention-test.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
// We need 99th percentile metrics, but PR has not been approved: https://github.com/benc-uk/k6-reporter/pull/65
import { htmlReport } from "https://raw.githubusercontent.com/masterkikoman/k6-reporter/refs/heads/main/dist/bundle.js"
// import { htmlReport } from "https://raw.githubusercontent.com/benc-uk/k6-reporter/main/dist/bundle.js";
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.1/index.js";

// Custom metrics for measuring contention
const transactionSuccess = new Rate('transaction_success');
const transactionRejected = new Counter('insufficient_balance_rejections');
const transactionRetries = new Counter('transaction_retries');
const transientErrors = new Counter('transient_errors');
const contentionRate = new Rate('contention_rate');

export const options = {
  scenarios: {
    contention_test: {
      executor: 'constant-arrival-rate',
      rate: 100,              // 200 iterations per timeUnit
      timeUnit: '1s',         // 200 iterations per second
      duration: '30s',         // Run for 5 minutes
      preAllocatedVUs: 100,    // Initial pool of VUs
      maxVUs: 100,            // Maximum pool of VUs
    },
  },
  
  // Performance thresholds
  thresholds: {
    'transaction_success': ['rate>0.7'],              // At least 70% of transactions should succeed
    'http_req_duration': ['p(95)<2000', 'p(99)<3000'], // 95% of requests under 2s, 99% under 3s
    'http_req_failed': ['rate<0.1'],                  // Less than 10% HTTP errors
    'insufficient_balance_rejections': ['count>0'],   // We expect some rejections due to insufficient balance
    'contention_rate': ['rate>0.3']                   // At least 30% of operations should experience contention
  },
};

// Base URL of the Fastify API server
const baseUrl = 'http://localhost:3001';

// Default function that k6 will call for each virtual user
export default function(data) {
  // Select one of the 100 shared wallets (SW1-SW100) randomly to create contention
  const walletIndex = Math.floor(Math.random() * 100) + 1;
  const walletId = `SW${walletIndex}`;
  
  // Use a random amount between 10-100 to deplete balances at varying rates
  const amount = Math.floor(Math.random() * 91) + 10;
  
  // Transaction details
  const maxRetries = 3;
  const retryDelay = 1;
  
  // Test ID for tracking this specific test iteration
  const testId = `vuser-${__VU}-${__ITER}-${Date.now()}`;
  
  // Body parameters for the request
  const payload = JSON.stringify({
    walletId: walletId,
    amount: amount,
    maxRetries: maxRetries,
    retryDelay: retryDelay,
    testId: testId
  });
  
  // Request headers
  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    tags: { 
      wallet_id: walletId,
    }
  };
  
  // Execute transaction
  const response = http.post(`${baseUrl}/execute-transaction`, payload, params);
  
  // Validate response
  const checkResult = check(response, {
    'status is 200': (r) => r.status === 200,
    'transaction completed': (r) => r.status === 200 && r.json().hasOwnProperty('success')
  });
  
  if (checkResult) {
    const result = response.json();
    
    // Track transaction success rate
    transactionSuccess.add(result.success);
    
    // Track insufficient balance rejections
    if (!result.success && result.message === "Insufficient balance") {
      transactionRejected.add(1);
      console.log(`Transaction ${testId} rejected - Insufficient balance for wallet ${walletId}`);
    }
    
    // Track retries
    if (result.retryCount && result.retryCount > 0) {
      transactionRetries.add(result.retryCount);
      contentionRate.add(1); // Count any retry as contention
      console.log(`Transaction ${testId} required ${result.retryCount} retries - contention on wallet ${walletId}`);
    }
    
    // Track transient errors
    if (result.error && result.error.includes("TransientTransactionError")) {
      transientErrors.add(1);
      contentionRate.add(1); // Count transient errors as contention
    }
    
    // Additional checks for transaction-specific outcomes
    check(result, {
      'transaction succeeded': (r) => r.success === true,
      'if failed, balance not negative': (r) => r.success === true || r.message === "Insufficient balance"
    });
  }
}

export function handleSummary(data) {
  const now = new Date();
  const timestamp = now.toISOString()
    .replace(/T/, '_')
    .replace(/\..+/, '')
    .replace(/:/g, '-');
  
  return {
    [`k6_reports/wallet_contention_${timestamp}.html`]: htmlReport(data),
    stdout: textSummary(data, { indent: " ", enableColors: true }),
  };
}