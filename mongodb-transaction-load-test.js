// mongodb-transaction-load-test.js
import http from 'k6/http';
import { check, group } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
// We need 99th percentile metrics, but PR has not been approved: https://github.com/benc-uk/k6-reporter/pull/65
import { htmlReport } from "https://raw.githubusercontent.com/masterkikoman/k6-reporter/refs/heads/main/dist/bundle.js"
// import { htmlReport } from "https://raw.githubusercontent.com/benc-uk/k6-reporter/main/dist/bundle.js";
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.1/index.js";

// Custom metrics
// Disclaimer: tags are not working now
const transactionSuccess = new Rate('transaction_success');
const transactionRetries = new Counter('transaction_retries');
const transientErrors = new Counter('transient_errors');

export const options = {
  // Test scenarios
  scenarios: {
    stress_rate: {
      executor: 'ramping-arrival-rate',
      timeUnit: '1s',
      preAllocatedVUs: 30,
      startRate: 150,
      // startRate: 10,

      stages: [
        // { target: 10, duration: '5s' },
        { target: 600, duration: '1m' },
        { target: 600, duration: '1h' },
        // Ramp down
        { target: 50, duration: '1m' }
      ],
    },
  },
  
  // Performance thresholds with added p99 thresholds
  thresholds: {
    'transaction_success': ['rate>0.95'],                // 95% of transactions should succeed
    'http_req_duration': ['p(95)<1000', 'p(99)<1500'],   // 95% of requests under 1s, 99% under 1.5s
    'http_req_failed': ['rate<0.05'],                    // Less than 5% HTTP errors
    'transaction_retries': ['count<100'],                // Fewer than 100 total retries in test
    'transient_errors': ['count<50'],                    // Fewer than 50 transient errors
  },
};

// Base URL of the Fastify API server
const baseUrl = 'http://localhost:3001';

// Shared function to execute transaction and process results
function executeTransaction(walletId, testId, walletType) {
  // Transaction details
  const amount = Math.floor(Math.random() * 20) + 1; // 1-20 units
  const maxRetries = 3;
  const retryDelay = 2;
  let sleepDuration = 0;
  
  // Body parameters for the request
  const payload = JSON.stringify({
    walletId: walletId,
    amount: amount,
    maxRetries: maxRetries,
    retryDelay: retryDelay,
    sleep: sleepDuration,
    testId: testId
  });
  
  // Request headers
  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    tags: { wallet_type: walletType }
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
    transactionSuccess.add(result.success, { wallet_type: walletType });
    
    // Track retries
    if (result.retryCount && result.retryCount > 0) {
      transactionRetries.add(result.retryCount, { wallet_type: walletType });
      console.log(`${walletType} - ${walletId} - Transaction ${testId} required ${result.retryCount} retries`);
    }
    
    // Track transient errors
    if (result.error && result.error.includes("TransientTransactionError")) {
      console.log(`${walletType} - ${walletId} - Transaction ${testId} experienced TransientTransactionError`);
      transientErrors.add(1, { wallet_type: walletType });
    }
    
    // Additional checks for transaction-specific outcomes
    check(result, {
      'transaction succeeded': (r) => r.success === true,
      'no retries needed': (r) => !r.retryCount || r.retryCount === 0
    });
  }
  
  return response;
}

// Default function that k6 will call for each virtual user
export default function() {
  // Test ID for tracking this specific test iteration
  const testId = `vuser-${__VU}-${__ITER}-${Date.now()}`;
  
  // Determine if this should use a shared wallet (for creating contention)
  const useSharedWallet = Math.random() < 0.3; // 30% chance to use shared wallet
  let walletType;
  let walletId; 
  
  if (useSharedWallet) {
    // Use a wallet from the shared pool (W1-W5000) to increase contention
    walletId = `W${Math.floor(Math.random() * 5000) + 1}`;
    walletType = "shared_wallet"
  } else {
    // Use a unique wallet ID between W5001-W100000
    walletId = `W${Math.floor(Math.random() * 95000) + 5001}`;
    walletType = "unique_wallet"
  }

  executeTransaction(walletId, testId, walletType);
}

export function handleSummary(data) {
  const now = new Date();
  const timestamp = now.toISOString()
    .replace(/T/, '_')
    .replace(/\..+/, '')
    .replace(/:/g, '-');

  return {
    [`k6_reports/load_test_${timestamp}.html`]: htmlReport(data, { showMarkers: true, }),
    stdout: textSummary(data, { indent: " ", enableColors: true, enablePercentiles: true }),
  };
}