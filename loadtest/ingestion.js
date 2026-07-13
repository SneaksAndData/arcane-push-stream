// k6 load test for the arcane-push-stream ingestion endpoint.
//
// Exercises the dynamic route backed by the `consumer2` DataRoute (Avro schema:
// { id: string, payload: map<string,string> }) by POSTing JSON payloads under
// increasing load, to observe how ingestion behaves under stress.
//
// Run (host must reach the server — see `just load-test` which port-forwards):
//   k6 run loadtest/ingestion.js
//
// Tunables (env vars):
//   BASE_URL     base URL of the server              (default http://localhost:8085)
//   API_VERSION  router apiVersion prefix            (default v2)
//   CONSUMER_ID  DataRoute consumerId                (default consumer2)
//   SCENARIO     ramp | constant | spike             (default ramp)
//   RATE         req/s for the `constant` scenario   (default 200)
//   DURATION     duration for the `constant` scenario (default 1m)
//
// Example — hammer at a fixed 500 rps for 2 minutes:
//   SCENARIO=constant RATE=500 DURATION=2m k6 run loadtest/ingestion.js

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8085';
const API_VERSION = __ENV.API_VERSION || 'v2';
const CONSUMER_ID = __ENV.CONSUMER_ID || 'consumer2';
const SCENARIO = __ENV.SCENARIO || 'ramp';
const RATE = parseInt(__ENV.RATE || '200', 10);
const DURATION = __ENV.DURATION || '1m';

const URL = `${BASE_URL}/api/${API_VERSION}/${CONSUMER_ID}/data`;

// Custom metrics for ingestion-specific outcomes.
const accepted = new Counter('ingest_accepted'); // HTTP 202
const rejected = new Counter('ingest_rejected'); // any non-202
const acceptedRate = new Rate('ingest_accepted_rate');

// Load profiles. Only the one named by SCENARIO is activated.
const scenarios = {
  // Gradually ramp VUs up and back down — good for finding the knee of the curve.
  ramp: {
    executor: 'ramping-vus',
    exec: 'ingest',
    startVUs: 0,
    stages: [
      { duration: '30s', target: 50 }, // warm up
      { duration: '1m', target: 200 }, // ramp to moderate load
      { duration: '1m', target: 500 }, // push harder
      { duration: '30s', target: 0 }, // ramp down
    ],
    gracefulRampDown: '10s',
  },

  // Hold a fixed request rate regardless of latency — decouples arrival rate
  // from response time so you can see queueing/backpressure behavior.
  constant: {
    executor: 'constant-arrival-rate',
    exec: 'ingest',
    rate: RATE,
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: Math.max(50, Math.ceil(RATE / 2)),
    maxVUs: Math.max(200, RATE * 2),
  },

  // Sudden spike then recovery — tests how the service copes with a burst.
  spike: {
    executor: 'ramping-arrival-rate',
    exec: 'ingest',
    startRate: 50,
    timeUnit: '1s',
    preAllocatedVUs: 100,
    maxVUs: 2000,
    stages: [
      { duration: '20s', target: 50 }, // baseline
      { duration: '10s', target: 1500 }, // spike
      { duration: '30s', target: 1500 }, // sustain spike
      { duration: '10s', target: 50 }, // recover
      { duration: '20s', target: 50 }, // settle
    ],
  },
};

export const options = {
  scenarios: { [SCENARIO]: scenarios[SCENARIO] },
  thresholds: {
    // 95% of requests under 200ms, 99% under 500ms.
    http_req_duration: ['p(95)<200', 'p(99)<500'],
    // Fewer than 1% transport-level failures.
    http_req_failed: ['rate<0.01'],
    // At least 99% of requests must be accepted (HTTP 202).
    ingest_accepted_rate: ['rate>0.99'],
  },
};

const params = {
  headers: { 'Content-Type': 'application/json' },
  tags: { endpoint: 'ingest', consumer: CONSUMER_ID },
};

// Build a unique-ish payload per iteration so records aren't identical.
function payload() {
  return JSON.stringify({
    id: `evt-${__VU}-${__ITER}-${randomIntBetween(1, 1e9)}`,
    payload: {
      k1: 'v1',
      k2: 'v2',
      vu: String(__VU),
      ts: String(Date.now()),
    },
  });
}

export function ingest() {
  const res = http.post(URL, payload(), params);

  const ok = res.status === 202;
  acceptedRate.add(ok);
  if (ok) {
    accepted.add(1);
  } else {
    rejected.add(1);
  }

  check(res, {
    'status is 202': (r) => r.status === 202,
    'body confirms acceptance': (r) => r.body && r.body.includes('accepted'),
  });
}

// Print the effective configuration once at startup.
export function setup() {
  console.log(`k6 ingestion load test -> ${URL}`);
  console.log(`scenario=${SCENARIO} (rate=${RATE}/s duration=${DURATION} where applicable)`);
}
