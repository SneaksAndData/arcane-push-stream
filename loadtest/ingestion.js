// k6 load test for the arcane-push-stream ingestion endpoint, shaped to
// resemble real production traffic rather than a synthetic best case.
//
// Target schema — `producer1` DataRoute (Producer1Event):
//   { id: ["null", string], payload: ["null", map<string,string>] }
// Validated server-side with Avro's jsonDecoder, which requires strict Avro
// JSON encoding: union values carry their branch name, so `id` is
// {"string": "..."} and `payload` is {"map": {...}}, or a bare null.
//
// What "realistic" means here:
//   - Runs against the deployed service over the network (TLS + real latency),
//     not a port-forwarded localhost socket.
//   - Mixed payload sizes: mostly small events, a long tail of large ones, and
//     a few near the 400 kB Content-Length limit the server enforces.
//   - Arrival-rate executors, so load does not throttle itself when the server
//     slows down — this is what exposes queueing and backpressure.
//   - Every documented response status is classified, so a run tells you *how*
//     it failed (throttled vs rejected vs server error), not just that it did.
//
// Run (requires a Boxer JWT, obtain with `snd login`):
//   SND_TOKEN='<jwt>' k6 run loadtest/ingestion.js
//
// Run against a local/port-forwarded server (no auth needed):
//   BASE_URL=http://localhost:8085 k6 run loadtest/ingestion.js
//
// Tunables (env vars):
//   SND_TOKEN    Boxer JWT; sent as `Authorization: Bearer` when set
//   BASE_URL     base URL of the server               (default https://arcane-push-stream-production-0.snd-awsp.io)
//   API_VERSION  router apiVersion prefix             (default v2)
//   CONSUMER_ID  DataRoute id(s), comma-separated     (default producer1)
//   SCENARIO     baseline | peak | spike | soak | breakpoint  (default baseline)
//   RATE         steady req/s for baseline/soak       (default 200)
//   PEAK_RATE    top req/s for peak/spike/breakpoint  (default 2000)
//   DURATION     hold time for baseline/soak          (default 5m)
//   STAGE        per-stage length for breakpoint      (default 2m; 6 stages)
//
// Examples:
//   SND_TOKEN=$T SCENARIO=peak PEAK_RATE=3000 k6 run loadtest/ingestion.js
//   SND_TOKEN=$T SCENARIO=breakpoint PEAK_RATE=5000 STAGE=20s k6 run loadtest/ingestion.js

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const TOKEN = __ENV.SND_TOKEN;
const BASE_URL = __ENV.BASE_URL || 'https://arcane-push-stream-production-0.snd-awsp.io';
const API_VERSION = __ENV.API_VERSION || 'v2';
const CONSUMER_IDS = (__ENV.CONSUMER_ID || 'producer1').split(',').map((s) => s.trim());
const SCENARIO = __ENV.SCENARIO || 'baseline';
const RATE = parseInt(__ENV.RATE || '200', 10);
const PEAK_RATE = parseInt(__ENV.PEAK_RATE || '2000', 10);
const DURATION = __ENV.DURATION || '5m';
const STAGE = __ENV.STAGE || '2m';

// Server-enforced cap (application.yaml: server.maxContentLengthBytes).
const MAX_CONTENT_LENGTH = 409600;

const urlFor = (consumerId) => `${BASE_URL}/api/${API_VERSION}/${consumerId}/data`;

// Outcome breakdown. Under real load the interesting question is not "did it
// fail" but "which way did it fail", so each documented status gets a counter.
const accepted = new Counter('ingest_accepted'); // 202
const throttled = new Counter('ingest_throttled'); // 429 (backpressure, once implemented)
const tooLarge = new Counter('ingest_too_large'); // 413
const badRequest = new Counter('ingest_bad_request'); // 400 / 415 / 411
const unauthorized = new Counter('ingest_unauthorized'); // 401 / 403
const serverError = new Counter('ingest_server_error'); // 5xx
const acceptedRate = new Rate('ingest_accepted_rate');
const throttledRate = new Rate('ingest_throttled_rate');
const payloadBytes = new Trend('ingest_payload_bytes');

// VU pool sized for the peak arrival rate: at ~100ms of latency each VU sustains
// roughly 10 rps, and headroom is needed when latency degrades under stress.
const preAllocatedVUs = Math.max(50, Math.ceil(PEAK_RATE / 10));
const maxVUs = Math.max(500, PEAK_RATE);

const scenarios = {
  // Steady production-like arrival rate. The reference run for comparing
  // deploys — regressions show up as latency/error drift at constant load.
  baseline: {
    executor: 'constant-arrival-rate',
    exec: 'ingest',
    rate: RATE,
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: Math.max(50, Math.ceil(RATE / 10)),
    maxVUs: Math.max(500, RATE * 5),
  },

  // A production day: quiet morning, business-hours climb, sustained peak,
  // wind-down. Catches problems that only appear after prolonged high load
  // (connection pool exhaustion, buffer growth, downstream throttling).
  peak: {
    executor: 'ramping-arrival-rate',
    exec: 'ingest',
    startRate: Math.ceil(PEAK_RATE / 20),
    timeUnit: '1s',
    preAllocatedVUs,
    maxVUs,
    stages: [
      { duration: '2m', target: Math.ceil(PEAK_RATE / 4) }, // morning ramp
      { duration: '3m', target: Math.ceil(PEAK_RATE / 2) }, // business hours
      { duration: '5m', target: PEAK_RATE }, // peak hour
      { duration: '3m', target: PEAK_RATE }, // sustained peak
      { duration: '2m', target: Math.ceil(PEAK_RATE / 20) }, // wind-down
    ],
  },

  // Batch job or upstream retry storm: near-instant jump to peak, then
  // recovery. Tests burst absorption and whether latency returns to normal.
  spike: {
    executor: 'ramping-arrival-rate',
    exec: 'ingest',
    startRate: Math.ceil(PEAK_RATE / 20),
    timeUnit: '1s',
    preAllocatedVUs,
    maxVUs,
    stages: [
      { duration: '1m', target: Math.ceil(PEAK_RATE / 20) }, // baseline
      { duration: '10s', target: PEAK_RATE }, // burst
      { duration: '2m', target: PEAK_RATE }, // sustain
      { duration: '10s', target: Math.ceil(PEAK_RATE / 20) }, // drop
      { duration: '3m', target: Math.ceil(PEAK_RATE / 20) }, // recovery window
    ],
  },

  // Long steady run for slow-burn issues: memory growth, file handle leaks,
  // credential/token expiry, downstream partition skew.
  soak: {
    executor: 'constant-arrival-rate',
    exec: 'ingest',
    rate: RATE,
    timeUnit: '1s',
    duration: __ENV.DURATION || '2h',
    preAllocatedVUs: Math.max(50, Math.ceil(RATE / 10)),
    maxVUs: Math.max(500, RATE * 5),
  },

  // Push until something gives, to find actual capacity. Six evenly spaced
  // steps from ~1/6 of PEAK_RATE up to PEAK_RATE; each step holds for STAGE so
  // the system has time to settle before the next increase. Latency and error
  // thresholds abort the run (see `options`), so the last completed step is the
  // last healthy rate — that is your capacity number.
  breakpoint: {
    executor: 'ramping-arrival-rate',
    exec: 'ingest',
    startRate: Math.ceil(PEAK_RATE / 6),
    timeUnit: '1s',
    preAllocatedVUs,
    maxVUs,
    stages: [1, 2, 3, 4, 5, 6].map((step) => ({
      duration: STAGE,
      target: Math.ceil((PEAK_RATE * step) / 6),
    })),
  },
};

// In `breakpoint` mode the thresholds are not pass/fail criteria — they are the
// stop condition. Crossing one means the current arrival rate is past what the
// service sustains, so the run aborts and the previous step is the answer.
// `delayAbortEval` avoids aborting on the noisy first seconds of a step.
const abort = (threshold) =>
  SCENARIO === 'breakpoint' ? { threshold, abortOnFail: true, delayAbortEval: '10s' } : threshold;

export const options = {
  scenarios: { [SCENARIO]: scenarios[SCENARIO] },

  // Real clients keep connections alive; disabling reuse would measure TLS
  // handshakes instead of ingestion.
  noConnectionReuse: false,
  discardResponseBodies: false,

  thresholds: {
    // Budgets for a remote endpoint over TLS — they include network RTT, which
    // on dev is a ~25ms floor, so these are looser than localhost numbers.
    http_req_duration: [abort('p(95)<500'), 'p(99)<1500'],
    // Server processing time only, excluding connect/TLS setup.
    http_req_waiting: [abort('p(95)<400')],
    http_req_failed: [abort('rate<0.01')],
    ingest_accepted_rate: [abort('rate>0.99')],
    // Backpressure is acceptable in small doses but not as a steady state.
    ingest_throttled_rate: [abort('rate<0.05')],
    ingest_server_error: ['count<10'],
    // Auth problems invalidate the whole run — fail fast rather than reporting
    // a meaningless "the server handled 5000 rps of 401s".
    ingest_unauthorized: [{ threshold: 'count<1', abortOnFail: true }],
  },
};

const authHeaders = TOKEN ? { Authorization: `Bearer ${TOKEN}` } : {};

// Production payloads are not uniform. Most events are small, but a long tail
// of larger ones stresses serialization, validation and the write path far more.
// Sizes are key counts; the resulting body stays under the 400 kB server limit.
function payloadKeyCount() {
  const roll = Math.random();
  if (roll < 0.7) return randomIntBetween(2, 10); // typical event
  if (roll < 0.95) return randomIntBetween(10, 100); // enriched event
  if (roll < 0.999) return randomIntBetween(100, 1000); // batch-ish event
  return randomIntBetween(3000, 17000); // rare event approaching the 400 kB cap
}

function payload() {
  const keys = payloadKeyCount();
  const map = { k1: 'v1', k2: 'v2', vu: String(__VU), ts: String(Date.now()) };
  for (let i = 0; i < keys; i++) {
    map[`f${i}`] = `v${i}-${randomIntBetween(1, 1e6)}`;
  }
  // Union fields are wrapped in their Avro branch name; a small share of events
  // send a null payload, which the schema allows and real producers do emit.
  const nullPayload = Math.random() < 0.02;
  const body = JSON.stringify({
    id: { string: `evt-${__VU}-${__ITER}-${randomIntBetween(1, 1e9)}` },
    payload: nullPayload ? null : { map },
  });
  // Guard the generator against the server's hard cap: exceeding it would
  // produce 413s that look like failures but are really test-harness bugs.
  return body.length > MAX_CONTENT_LENGTH
    ? JSON.stringify({
        id: { string: `evt-${__VU}-${__ITER}` },
        payload: { map: { k1: 'v1', k2: 'v2' } },
      })
    : body;
}

export function ingest() {
  // Spread load across routes when several are configured, as in production
  // where many producers share the service.
  const consumerId = CONSUMER_IDS[randomIntBetween(0, CONSUMER_IDS.length - 1)];
  const body = payload();
  payloadBytes.add(body.length);

  const res = http.post(urlFor(consumerId), body, {
    headers: Object.assign({ 'Content-Type': 'application/json' }, authHeaders),
    tags: { endpoint: 'ingest', consumer: consumerId },
  });

  const ok = res.status === 202;
  acceptedRate.add(ok);
  throttledRate.add(res.status === 429);

  // A token the cluster cannot decrypt (e.g. a dev token against prod) comes
  // back as 500 "Invalid key format", not 401 — without this it would be
  // miscounted as a server error and the auth abort would never fire.
  const authFailed =
    res.status === 401 ||
    res.status === 403 ||
    (res.status === 500 && !!res.body && /Invalid (JWT|key) format/i.test(res.body));

  if (ok) accepted.add(1);
  else if (res.status === 429) throttled.add(1);
  else if (res.status === 413) tooLarge.add(1);
  else if (authFailed) unauthorized.add(1);
  else if (res.status >= 500) serverError.add(1);
  else if (res.status >= 400) badRequest.add(1);

  check(res, {
    'status is 202': (r) => r.status === 202,
    'body confirms acceptance': (r) => !!r.body && r.body.includes('accepted for'),
    'no server error': (r) => r.status < 500,
    'authorized': () => !authFailed,
  });
}

export function setup() {
  console.log(`k6 ingestion load test -> ${urlFor(CONSUMER_IDS.join('|'))}`);
  console.log(`scenario=${SCENARIO} rate=${RATE}/s peak=${PEAK_RATE}/s duration=${DURATION}`);
  console.log(`auth=${TOKEN ? 'bearer token' : 'none (SND_TOKEN unset)'}`);
  if (SCENARIO === 'breakpoint') {
    // Printed so an abort timestamp can be mapped back to the rate that caused it.
    const steps = scenarios.breakpoint.stages.map((s) => `${s.target}/s`).join(' -> ');
    console.log(`breakpoint steps (${STAGE} each): ${steps}`);
  }
  if (!TOKEN && BASE_URL.startsWith('https://')) {
    console.warn('SND_TOKEN is unset against a remote host — expect 401/403. Run `snd login`.');
  }
}
