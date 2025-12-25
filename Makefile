.PHONY: help install dev-install lint format typecheck test itest e2e test-all cov \
        deps-up deps-down up down logs demo demo-large loadtest loadtest-heavy clean

# Default target
help:
	@echo "Risk Scoring Pipeline - Development Commands"
	@echo ""
	@echo "Setup:"
	@echo "  install      Install production dependencies"
	@echo "  dev-install  Install dev dependencies"
	@echo ""
	@echo "Quality:"
	@echo "  lint         Run ruff linter"
	@echo "  format       Format code with ruff"
	@echo "  typecheck    Run mypy type checker"
	@echo ""
	@echo "Testing:"
	@echo "  test         Run unit + contract tests (fast, no docker)"
	@echo "  itest        Run integration tests (requires docker deps)"
	@echo "  e2e          Run end-to-end tests (requires docker deps)"
	@echo "  test-all     Run all tests"
	@echo "  cov          Run tests with coverage report"
	@echo ""
	@echo "Infrastructure:"
	@echo "  deps-up      Start Postgres + Redpanda"
	@echo "  deps-down    Stop and remove containers + volumes"
	@echo "  up           Start full stack (deps + services)"
	@echo "  down         Stop full stack"
	@echo "  logs         Tail logs from all containers"
	@echo ""
	@echo "Demo & Load Testing:"
	@echo "  demo            Generate 5 users with sample events"
	@echo "  demo-large      Generate 100 users with sample events"
	@echo "  loadtest        Run k6 load test (10 VUs, 30s)"
	@echo "  loadtest-heavy  Run heavy load test (50 VUs, 60s)"
	@echo ""
	@echo "Misc:"
	@echo "  clean        Remove build artifacts and caches"

# ============================================================================
# Setup
# ============================================================================
install:
	pip install -e .

dev-install:
	pip install -e ".[dev]"

# ============================================================================
# Code quality
# ============================================================================
lint:
	ruff check .

format:
	ruff format .
	ruff check --fix .

typecheck:
	mypy services/ shared/

# ============================================================================
# Testing
# ============================================================================
test:
	pytest -m "unit or contract"

itest: deps-up
	pytest -m integration

e2e: deps-up
	pytest -m e2e

test-all: deps-up
	pytest

cov:
	pytest --cov --cov-report=term-missing --cov-report=html

# ============================================================================
# Infrastructure
# ============================================================================
COMPOSE_FILE := infra/docker-compose.yml

deps-up:
	docker compose -f $(COMPOSE_FILE) up -d postgres redpanda
	@echo "Waiting for services to be ready..."
	@sleep 3
	@docker compose -f $(COMPOSE_FILE) exec -T redpanda rpk cluster health --watch --exit-when-healthy || true

deps-down:
	docker compose -f $(COMPOSE_FILE) down -v

up:
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

# Create Kafka topics (run after deps-up)
topics:
	docker compose -f $(COMPOSE_FILE) exec -T redpanda rpk topic create risk.events -p 3 || true
	docker compose -f $(COMPOSE_FILE) exec -T redpanda rpk topic create risk.events.dlq -p 1 || true
	docker compose -f $(COMPOSE_FILE) exec -T redpanda rpk topic list

# ============================================================================
# Database migrations
# ============================================================================
migrate:
	alembic upgrade head

migrate-down:
	alembic downgrade -1

migrate-new:
	@read -p "Migration message: " msg; alembic revision --autogenerate -m "$$msg"

# ============================================================================
# Demo and load testing
# ============================================================================
demo:
	@echo "Running demo (generates events and shows example queries)..."
	python scripts/demo.py --users 5 --events-per-user 10

demo-large:
	@echo "Running large demo (100 users, 20 events each)..."
	python scripts/demo.py --users 100 --events-per-user 20

loadtest:
	@echo "Running load test (10 VUs, 30s)..."
	@command -v k6 >/dev/null 2>&1 || { echo "k6 not installed. Install with: brew install k6"; exit 1; }
	k6 run scripts/loadtest.js

loadtest-heavy:
	@echo "Running heavy load test (50 VUs, 60s)..."
	@command -v k6 >/dev/null 2>&1 || { echo "k6 not installed. Install with: brew install k6"; exit 1; }
	K6_VUS=50 K6_DURATION=60s k6 run scripts/loadtest.js

# ============================================================================
# Cleanup
# ============================================================================
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
