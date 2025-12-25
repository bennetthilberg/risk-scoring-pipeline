/**
 * k6 Load Test Script for Risk Scoring Pipeline
 *
 * Tests the event ingestion endpoint under load and measures:
 * - Throughput (requests per second)
 * - Latency (p50, p90, p95, p99)
 * - Error rate
 *
 * Usage:
 *   k6 run scripts/loadtest.js
 *   k6 run scripts/loadtest.js --vus 50 --duration 30s
 *   K6_API_URL=http://api:8000 k6 run scripts/loadtest.js
 *
 * Environment variables:
 *   K6_API_URL      - API base URL (default: http://localhost:8000)
 *   K6_VUS          - Number of virtual users (default: 10)
 *   K6_DURATION     - Test duration (default: 30s)
 *   K6_RAMP_UP      - Ramp-up time (default: 5s)
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend, Counter } from "k6/metrics";
import { randomIntBetween, uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

const API_URL = __ENV.K6_API_URL || "http://localhost:8000";

const eventsIngested = new Counter("events_ingested");
const eventsFailed = new Counter("events_failed");
const eventLatency = new Trend("event_latency", true);
const errorRate = new Rate("errors");

export const options = {
  stages: [
    { duration: __ENV.K6_RAMP_UP || "5s", target: parseInt(__ENV.K6_VUS) || 10 },
    { duration: __ENV.K6_DURATION || "30s", target: parseInt(__ENV.K6_VUS) || 10 },
    { duration: "5s", target: 0 },
  ],
  thresholds: {
    http_req_duration: ["p(95)<500", "p(99)<1000"],
    errors: ["rate<0.05"],
    http_req_failed: ["rate<0.05"],
  },
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

const EMAIL_DOMAINS = [
  "gmail.com",
  "yahoo.com",
  "hotmail.com",
  "protonmail.com",
  "outlook.com",
  "temp-mail.org",
];

const COUNTRIES = ["US", "CA", "GB", "DE", "FR", "JP", "AU", "BR"];

const MERCHANTS = [
  "Amazon",
  "Walmart",
  "Target",
  "BestBuy",
  "Apple Store",
  "SketchyShop",
];

const DEVICE_IDS = [
  "device-iphone-001",
  "device-android-002",
  "device-web-003",
  "device-tablet-004",
];

function randomChoice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function generateSignupEvent(userId) {
  return {
    event_id: uuidv4(),
    event_type: "signup",
    user_id: userId,
    ts: new Date().toISOString(),
    schema_version: 1,
    payload: {
      email_domain: randomChoice(EMAIL_DOMAINS),
      country: randomChoice(COUNTRIES),
      device_id: randomChoice(DEVICE_IDS),
    },
  };
}

function generateLoginEvent(userId) {
  const ipOctets = [
    randomIntBetween(10, 223),
    randomIntBetween(0, 255),
    randomIntBetween(0, 255),
    randomIntBetween(1, 254),
  ];

  return {
    event_id: uuidv4(),
    event_type: "login",
    user_id: userId,
    ts: new Date().toISOString(),
    schema_version: 1,
    payload: {
      ip: ipOctets.join("."),
      success: Math.random() > 0.1,
      device_id: randomChoice(DEVICE_IDS),
    },
  };
}

function generateTransactionEvent(userId) {
  const isHighValue = Math.random() < 0.1;
  const amount = isHighValue
    ? randomIntBetween(500, 5000)
    : randomIntBetween(5, 200);

  return {
    event_id: uuidv4(),
    event_type: "transaction",
    user_id: userId,
    ts: new Date().toISOString(),
    schema_version: 1,
    payload: {
      amount: amount + Math.random(),
      currency: "USD",
      merchant: randomChoice(MERCHANTS),
      country: randomChoice(COUNTRIES),
    },
  };
}

function generateRandomEvent(userId) {
  const eventGenerators = [
    generateSignupEvent,
    generateLoginEvent,
    generateLoginEvent,
    generateTransactionEvent,
    generateTransactionEvent,
    generateTransactionEvent,
  ];
  return randomChoice(eventGenerators)(userId);
}

export function setup() {
  console.log(`\n${"=".repeat(60)}`);
  console.log("  Risk Scoring Pipeline Load Test");
  console.log(`${"=".repeat(60)}`);
  console.log(`  API URL:    ${API_URL}`);
  console.log(`  VUs:        ${parseInt(__ENV.K6_VUS) || 10}`);
  console.log(`  Duration:   ${__ENV.K6_DURATION || "30s"}`);
  console.log(`${"=".repeat(60)}\n`);

  const healthRes = http.get(`${API_URL}/health`);
  if (healthRes.status !== 200) {
    console.error(`Health check failed: ${healthRes.status}`);
    console.error("Make sure the API is running: make up");
    return { healthy: false };
  }

  console.log("API health check: OK\n");
  return { healthy: true, startTime: Date.now() };
}

export default function (data) {
  if (!data.healthy) {
    sleep(1);
    return;
  }

  const userId = `user-load-${__VU}-${randomIntBetween(1, 100)}`;
  const event = generateRandomEvent(userId);

  const startTime = Date.now();
  const res = http.post(`${API_URL}/events`, JSON.stringify(event), {
    headers: {
      "Content-Type": "application/json",
    },
    tags: { endpoint: "events", event_type: event.event_type },
  });
  const duration = Date.now() - startTime;

  eventLatency.add(duration);

  const success = check(res, {
    "status is 200 or 202": (r) => r.status === 200 || r.status === 202,
    "response time < 500ms": (r) => r.timings.duration < 500,
    "has event_id in response": (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.event_id !== undefined;
      } catch {
        return false;
      }
    },
  });

  if (success) {
    eventsIngested.add(1);
    errorRate.add(0);
  } else {
    eventsFailed.add(1);
    errorRate.add(1);
  }

  sleep(Math.random() * 0.1);
}

export function teardown(data) {
  if (!data.healthy) {
    console.log("\nLoad test skipped - API was not healthy");
    return;
  }

  const elapsed = (Date.now() - data.startTime) / 1000;

  console.log(`\n${"=".repeat(60)}`);
  console.log("  Load Test Summary");
  console.log(`${"=".repeat(60)}`);
  console.log(`  Total Duration:     ${elapsed.toFixed(1)}s`);
  console.log(`${"=".repeat(60)}\n`);
}

export function handleSummary(data) {
  const httpReqs = data.metrics.http_reqs;
  const duration = data.metrics.http_req_duration;
  const errors = data.metrics.errors;

  const totalRequests = httpReqs ? httpReqs.values.count : 0;
  const rps = httpReqs ? httpReqs.values.rate : 0;

  const p50 = duration ? duration.values.med : 0;
  const p90 = duration ? duration.values["p(90)"] : 0;
  const p95 = duration ? duration.values["p(95)"] : 0;
  const p99 = duration ? duration.values["p(99)"] : 0;
  const avg = duration ? duration.values.avg : 0;

  const errorRateVal = errors ? errors.values.rate * 100 : 0;

  const summary = `
${"=".repeat(60)}
  LOAD TEST RESULTS
${"=".repeat(60)}

  Throughput:
    Total Requests:    ${totalRequests}
    Requests/sec:      ${rps.toFixed(2)}

  Latency (ms):
    Average:           ${avg.toFixed(2)}
    Median (p50):      ${p50.toFixed(2)}
    p90:               ${p90.toFixed(2)}
    p95:               ${p95.toFixed(2)}
    p99:               ${p99.toFixed(2)}

  Reliability:
    Error Rate:        ${errorRateVal.toFixed(2)}%

${"=".repeat(60)}
`;

  console.log(summary);

  const jsonSummary = {
    timestamp: new Date().toISOString(),
    config: {
      api_url: API_URL,
      vus: parseInt(__ENV.K6_VUS) || 10,
      duration: __ENV.K6_DURATION || "30s",
    },
    results: {
      total_requests: totalRequests,
      requests_per_second: rps,
      latency_ms: {
        avg: avg,
        p50: p50,
        p90: p90,
        p95: p95,
        p99: p99,
      },
      error_rate_percent: errorRateVal,
    },
    passed: errorRateVal < 5 && p95 < 500,
  };

  return {
    stdout: summary,
    "loadtest-results.json": JSON.stringify(jsonSummary, null, 2),
  };
}
