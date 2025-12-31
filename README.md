# Real-time Risk Scoring Platform

A production-grade streaming pipeline that ingests user events (signups, logins, and transactions), computes features, produces risk scores using a trained ML model, and exposes query APIs with observability dashboards.

## Architecture

```
                                     ┌─────────────────────────────────────────────────────────┐
                                     │                    Risk Scoring Platform                │
                                     └─────────────────────────────────────────────────────────┘

┌──────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌──────────┐
│  Client  │────▶│    API Service  │────▶│     Redpanda    │────▶│  Scorer Worker  │────▶│ Postgres │
│          │     │    (FastAPI)    │     │     (Kafka)     │     │   (Consumer)    │     │          │
└──────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘     └──────────┘
                        │                                               │
                        │                                               │
                        ▼                                               ▼
                 ┌──────────┐                                    ┌──────────────┐
                 │ Postgres │                                    │ ML Model     │
                 │ (events) │                                    │ (model.pkl)  │
                 └──────────┘                                    └──────────────┘
```

**Data Flow:**
1. Client sends event to `POST /events`
2. API validates event, stores in `events` table, publishes to `risk.events` topic
3. Scorer worker consumes event, computes features from historical data
4. Model predicts risk score (0-1) with band (low/med/high)
5. Score stored in `risk_scores` table, queryable via `GET /score/{user_id}`

## Quickstart

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Make (optional but recommended)

### 1. Clone and Setup

```bash
git clone <repository-url>
cd risk-scoring-pipeline

python -m venv .venv
source .venv/bin/activate

pip install -e ".[dev]"
```

### 2. Start Infrastructure

```bash
make deps-up

make topics

make migrate
```

### 3. Optionally Train the Model

```bash
python scripts/train.py
```

### 4. Start Services

```bash
# Option 1: Start all services with Docker
make up

# Option 2: Run services locally for development
uvicorn services.api.main:app --reload --port 8000

python -m services.scorer.main
```

### 5. Verify Installation

```bash
curl http://localhost:8000/health

make smoke
```

### 6. Run Demo

```bash
make demo
```

## API Endpoints

### Health & Metrics

`/health` | GET | Health check, returns `{"status": "healthy", "version": "0.1.0"}`
`/metrics` | GET | Prometheus metrics in text format

### Events

`/events` | POST | Ingest an event (signup, login, or transaction)

**Request Body:**
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

**Event Payloads:**

Signup:
```json
{"email_domain": "gmail.com", "country": "US", "device_id": "device-123"}
```

Login:
```json
{"ip": "192.168.1.1", "success": true, "device_id": "device-123"}
```

Transaction:
```json
{"amount": 100.00, "currency": "USD", "merchant": "Amazon", "country": "US"}
```

### Scores

`/score/{user_id}` | GET | Get latest risk score for a user

**Response:**
```json
{
  "user_id": "user-001",
  "score": 0.234,
  "band": "low",
  "computed_at": "2024-01-15T12:00:00Z",
  "top_features": {
    "txn_amount_sum_24h": 0.156,
    "failed_logins_1h": 0.089
  }
}
```

### DLQ

`/dlq` | GET | List DLQ entries (failed events)
`/dlq/{id}` | GET | Get specific DLQ entry

## Risk Bands

| Band | Score Range | Description |
|------|-------------|-------------|
| `low` | 0.00 - 0.33 | Low risk user |
| `med` | 0.33 - 0.66 | Medium risk, may need monitoring |
| `high` | 0.66 - 1.00 | High risk, potential fraud |

## Features

The model uses these computed features:

| Feature | Description | Window |
|---------|-------------|--------|
| `txn_count_24h` | Number of transactions | 24 hours |
| `txn_amount_sum_24h` | Total transaction amount | 24 hours |
| `failed_logins_1h` | Failed login attempts | 1 hour |
| `account_age_days` | Days since signup | All time |
| `unique_countries_7d` | Distinct transaction countries | 7 days |
| `avg_txn_amount_30d` | Average transaction amount | 30 days |

## Development

### Run Tests

```bash
# Unit and contract tests (no docker needed)
make test

# Integration tests (requires docker)
make itest

# End-to-end tests
make e2e

# Smoke tests
make smoke

# All tests
make test-all

# With coverage
make cov
```

### Code Quality

```bash
# Lint
make lint

# Format
make format

# Type check
make typecheck
```

### Load Testing

```bash
# Standard load test (10 VUs, 30s)
make loadtest

# Heavy load test (50 VUs, 60s)
make loadtest-heavy
```

## Configuration

Environment variables (see `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/riskdb` | PostgreSQL connection |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka/Redpanda brokers |
| `KAFKA_TOPIC` | `risk.events` | Main event topic |
| `DLQ_TOPIC` | `risk.events.dlq` | Dead letter queue topic |
| `CONSUMER_GROUP` | `risk-scorer` | Kafka consumer group |
| `MODEL_PATH` | `models/` | Path to model artifacts |
| `MAX_RETRIES` | `3` | Max retry attempts |
| `RETRY_BASE_DELAY_MS` | `100` | Base backoff delay |

## Failure Handling

### Idempotency

- **API**: `events.event_id` is UNIQUE; duplicate POSTs are safely handled
- **Worker**: `processed_events.event_id` is UNIQUE; replayed messages are skipped

### Retry Policy

- Transient errors: retry with exponential backoff
- Nonretryable errors: send to DLQ immediately
- After n retries: send to DLQ with failure reason

### Dead Letter Queue

Failed events are stored in `dlq_events` table with:
- Original payload
- Failure reason
- Retry count
- Timestamp

View via `GET /dlq` or Redpanda Console.

## Observability

### Metrics (Prometheus)

API metrics:
- `http_requests_total{method, endpoint, status}` - Request count
- `http_request_duration_seconds{method, endpoint}` - Request latency

Worker metrics:
- `events_processed_total{event_type, status}` - Events processed
- `scoring_duration_seconds{model_version}` - Scoring latency
- `dlq_events_total{reason}` - DLQ events
- `consumer_lag{topic, partition}` - Kafka consumer lag

### Grafana Dashboard

Access at `http://localhost:3000` (admin/admin) when running with `--profile full`:

```bash
docker compose -f infra/docker-compose.yml --profile full up -d
```

## Structure

```
risk-scoring-pipeline/
├── services/
│   ├── api/           # FastAPI ingest service
│   │   ├── main.py
│   │   ├── routes/
│   │   └── dependencies.py
│   └── scorer/        # Kafka consumer worker
│       ├── main.py
│       ├── processor.py
│       ├── features.py
│       └── scoring.py
├── shared/            # Shared code (schemas, DB models, config)
│   ├── schemas.py
│   ├── db.py
│   ├── config.py
│   └── model.py
├── migrations/        # Alembic database migrations
├── models/            # Trained model artifacts
├── scripts/           # Demo and training scripts
├── tests/             # Test suite
│   ├── unit/
│   ├── contract/
│   ├── integration/
│   ├── e2e/
│   └── smoke/
├── infra/             # Docker Compose and configs
├── dashboards/        # Grafana dashboard JSON
└── Makefile
```

## Make Targets

```
Setup:
  install      Install production dependencies
  dev-install  Install dev dependencies

Quality:
  lint         Run ruff linter
  format       Format code with ruff
  typecheck    Run mypy type checker

Testing:
  test         Run unit + contract tests (fast, no docker)
  itest        Run integration tests (requires docker deps)
  e2e          Run end-to-end tests (requires docker deps)
  smoke        Run smoke tests validating quickstart (requires docker deps)
  test-all     Run all tests
  cov          Run tests with coverage report

Infrastructure:
  deps-up      Start Postgres + Redpanda
  deps-down    Stop and remove containers + volumes
  up           Start full stack (deps + services)
  down         Stop full stack
  logs         Tail logs from all containers

Demo & Load Testing:
  demo            Generate 5 users with sample events
  demo-large      Generate 100 users with sample events
  loadtest        Run k6 load test (10 VUs, 30s)
  loadtest-heavy  Run heavy load test (50 VUs, 60s)
```

## License

MIT