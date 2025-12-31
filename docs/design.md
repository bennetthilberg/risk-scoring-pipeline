# Risk Scoring Platform - Design Document

## Overview

A real-time risk scoring platform that ingests user events (signup, login, transaction), computes risk features, scores users with a trained ML model, and exposes query APIs for risk assessment.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Clients                                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         API Service (FastAPI)                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ POST /events│  │GET /score/  │  │ GET /health │  │   GET /metrics      │ │
│  │             │  │  {user_id}  │  │             │  │   (Prometheus)      │ │
│  └──────┬──────┘  └──────┬──────┘  └─────────────┘  └─────────────────────┘ │
│         │                │                                                   │
│         ▼                ▼                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                    PostgreSQL (events, risk_scores)                     ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│         │                                                                    │
│         ▼                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                    Kafka/Redpanda (risk.events)                         ││
│  └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Scoring Worker (Python)                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │   Consume   │─▶│  Compute    │─▶│    Score    │─▶│   Write to DB       │ │
│  │   Events    │  │  Features   │  │  (sklearn)  │  │   (risk_scores)     │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────┘ │
│         │                                                                    │
│         ▼ (on failure)                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                    DLQ (risk.events.dlq)                                ││
│  └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

1. **Event Ingestion**
   - Client sends event to `POST /events`
   - API validates event against Pydantic schema
   - API inserts row into `events` table (immutable audit log)
   - API publishes event to `risk.events` Kafka topic
   - API returns 202 Accepted

2. **Event Processing**
   - Worker consumes from `risk.events` topic
   - Worker checks `processed_events` for dedupe
   - Worker computes features from historical events
   - Worker scores features using trained model
   - Worker writes to `risk_scores` table
   - Worker records in `processed_events`
   - Worker commits Kafka offset

3. **Score Query**
   - Client queries `GET /score/{user_id}`
   - API returns latest score, band, and top features

## Database Schema

### events (immutable audit log)

| Column            | Type         | Notes                          |
|-------------------|--------------|--------------------------------|
| event_id          | UUID PK      | Client-provided, UNIQUE        |
| user_id           | VARCHAR(64)  | Indexed                        |
| event_type        | VARCHAR(32)  | signup, login, transaction     |
| ts                | TIMESTAMPTZ  | Event timestamp (client)       |
| schema_version    | INTEGER      | Schema version                 |
| payload_json      | JSONB        | Event-specific payload         |
| raw_payload_hash  | VARCHAR(64)  | SHA-256 for integrity          |
| accepted_at       | TIMESTAMPTZ  | Server receipt time            |
| published_at      | TIMESTAMPTZ  | Kafka publish time (nullable)  |

Indexes: `(user_id, ts)`, `(event_type)`

### risk_scores

| Column            | Type         | Notes                          |
|-------------------|--------------|--------------------------------|
| id                | SERIAL PK    | Internal ID                    |
| user_id           | VARCHAR(64)  | Indexed                        |
| score             | FLOAT        | Risk score [0, 1]              |
| band              | VARCHAR(8)   | low, med, high                 |
| computed_at       | TIMESTAMPTZ  | Score computation time         |
| model_version     | VARCHAR(32)  | Model artifact version         |
| top_features_json | JSONB        | Feature contributions          |

Indexes: `(user_id, computed_at DESC)`

### processed_events (worker dedupe)

| Column            | Type         | Notes                          |
|-------------------|--------------|--------------------------------|
| event_id          | UUID PK      | UNIQUE constraint for dedupe   |
| processed_at      | TIMESTAMPTZ  | Processing completion time     |
| status            | VARCHAR(16)  | success, failed, dlq           |

### model_versions (optional tracking)

| Column            | Type         | Notes                          |
|-------------------|--------------|--------------------------------|
| model_version     | VARCHAR(32)  | Version string (PK)            |
| created_at        | TIMESTAMPTZ  | Training time                  |
| params_hash       | VARCHAR(64)  | Hash of training params        |
| feature_order     | JSONB        | Ordered feature names          |
| band_thresholds   | JSONB        | {low: 0.33, high: 0.66}        |

## Event Schema

### Envelope

```json
{
  "event_id": "uuid",
  "event_type": "signup|login|transaction",
  "user_id": "string",
  "ts": "ISO-8601 timestamp",
  "schema_version": 1,
  "payload": { ... }
}
```

### Payload Types

**SignupEvent**
```json
{
  "email_domain": "string",
  "country": "string (ISO 3166-1 alpha-2)",
  "device_id": "string"
}
```

**LoginEvent**
```json
{
  "ip": "string (IPv4/IPv6)",
  "success": "boolean",
  "device_id": "string"
}
```

**TransactionEvent**
```json
{
  "amount": "float",
  "currency": "string (ISO 4217)",
  "merchant": "string",
  "country": "string (ISO 3166-1 alpha-2)"
}
```

## Feature Computation

Features computed from event history (rolling windows):

| Feature              | Window  | Description                        |
|----------------------|---------|------------------------------------|
| txn_count_24h        | 24h     | Transaction count                  |
| txn_amount_sum_24h   | 24h     | Total transaction amount           |
| failed_logins_1h     | 1h      | Failed login attempts              |
| account_age_days     | -       | Days since signup                  |
| unique_countries_7d  | 7d      | Unique countries in transactions   |
| avg_txn_amount_30d   | 30d     | Average transaction amount         |

## Failure Handling

### Idempotency

- **API**: `events.event_id` has UNIQUE constraint; upsert ignores duplicates
- **Worker**: `processed_events.event_id` has UNIQUE constraint; skip if exists

### Retry Policy

- Retryable errors: DB connection failures, transient Kafka errors
- Non-retryable errors: Schema validation, unknown event types
- Max retries: 3
- Backoff: Exponential (100ms, 200ms, 400ms)

### Dead Letter Queue

Events are sent to `risk.events.dlq` when:
- Max retries exceeded for retryable errors
- Non-retryable error (immediate DLQ)

DLQ message includes:
- Original event payload
- Error message
- Retry count
- Timestamp

## API Endpoints

| Endpoint              | Method | Description                        |
|-----------------------|--------|------------------------------------|
| `/events`             | POST   | Ingest event                       |
| `/score/{user_id}`    | GET    | Get latest risk score              |
| `/health`             | GET    | Health check                       |
| `/metrics`            | GET    | Prometheus metrics                 |

## Configuration

Environment variables:
- `DATABASE_URL`: PostgreSQL connection string
- `KAFKA_BROKERS`: Comma-separated broker addresses
- `KAFKA_TOPIC`: Events topic (default: risk.events)
- `DLQ_TOPIC`: DLQ topic (default: risk.events.dlq)
- `MODEL_PATH`: Path to model artifact

## Observability

### Metrics (Prometheus)

**API Service**
- `http_requests_total{method, endpoint, status}` - Request count
- `http_request_duration_seconds{method, endpoint}` - Latency histogram
- `events_accepted_total` - Events accepted
- `events_published_total` - Events published to Kafka

**Worker Service**
- `events_processed_total{status}` - Events processed
- `scoring_duration_seconds` - Model inference time
- `dlq_events_total` - Events sent to DLQ
- `consumer_lag` - Kafka consumer lag

### Dashboards

Grafana dashboards in `dashboards/`:
- Request rate and error rate
- P50/P95/P99 latency
- Consumer lag
- DLQ counts
- Score distribution by band
