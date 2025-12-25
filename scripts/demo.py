"""
Demo script for the Risk Scoring Pipeline.

Generates synthetic users and events, sends them to the API,
and prints example curl commands for exploration.

Usage:
    python scripts/demo.py [--users N] [--events-per-user M] [--api-url URL]
    make demo
"""

import argparse
import random
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx


@dataclass
class DemoConfig:
    api_url: str
    num_users: int
    events_per_user: int
    seed: int
    delay_ms: int


EMAIL_DOMAINS = [
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "protonmail.com",
    "outlook.com",
    "icloud.com",
    "mail.ru",
    "temp-mail.org",
]

COUNTRIES = ["US", "CA", "GB", "DE", "FR", "JP", "AU", "BR", "IN", "NG"]

MERCHANTS = [
    "Amazon",
    "Walmart",
    "Target",
    "BestBuy",
    "Steam",
    "Apple Store",
    "SketchyElectronics",
    "CryptoExchange",
    "FastCash",
    "Unknown Merchant",
]

DEVICE_IDS = [
    "device-iphone-001",
    "device-android-002",
    "device-web-003",
    "device-unknown-999",
]


def generate_user_id(index: int) -> str:
    return f"user-demo-{index:04d}"


def generate_event_id() -> str:
    return str(uuid.uuid4())


def generate_timestamp(base_time: datetime, offset_minutes: int) -> str:
    ts = base_time + timedelta(minutes=offset_minutes)
    return ts.isoformat()


def generate_signup_event(
    user_id: str,
    timestamp: str,
    rng: random.Random,
    is_risky: bool = False,
) -> dict[str, Any]:
    if is_risky:
        email_domain = rng.choice(["temp-mail.org", "mail.ru", "disposable.com"])
        country = rng.choice(["NG", "RU", "CN"])
    else:
        email_domain = rng.choice(["gmail.com", "yahoo.com", "outlook.com"])
        country = rng.choice(["US", "CA", "GB", "DE"])

    return {
        "event_id": generate_event_id(),
        "event_type": "signup",
        "user_id": user_id,
        "ts": timestamp,
        "schema_version": 1,
        "payload": {
            "email_domain": email_domain,
            "country": country,
            "device_id": rng.choice(DEVICE_IDS),
        },
    }


def generate_login_event(
    user_id: str,
    timestamp: str,
    rng: random.Random,
    is_risky: bool = False,
) -> dict[str, Any]:
    if is_risky:
        success = rng.random() < 0.3
        ip_prefix = rng.choice(["185.", "91.", "45."])
    else:
        success = rng.random() < 0.95
        ip_prefix = rng.choice(["192.168.", "10.0.", "172.16."])

    ip = f"{ip_prefix}{rng.randint(0, 255)}.{rng.randint(1, 254)}"

    return {
        "event_id": generate_event_id(),
        "event_type": "login",
        "user_id": user_id,
        "ts": timestamp,
        "schema_version": 1,
        "payload": {
            "ip": ip,
            "success": success,
            "device_id": rng.choice(DEVICE_IDS),
        },
    }


def generate_transaction_event(
    user_id: str,
    timestamp: str,
    rng: random.Random,
    is_risky: bool = False,
) -> dict[str, Any]:
    if is_risky:
        amount = rng.uniform(500, 5000)
        merchant = rng.choice(["CryptoExchange", "FastCash", "SketchyElectronics"])
        country = rng.choice(["NG", "RU", "CN"])
    else:
        amount = rng.uniform(5, 200)
        merchant = rng.choice(["Amazon", "Walmart", "Target", "BestBuy"])
        country = rng.choice(["US", "CA", "GB"])

    return {
        "event_id": generate_event_id(),
        "event_type": "transaction",
        "user_id": user_id,
        "ts": timestamp,
        "schema_version": 1,
        "payload": {
            "amount": round(amount, 2),
            "currency": "USD",
            "merchant": merchant,
            "country": country,
        },
    }


def generate_user_event_sequence(
    user_id: str,
    num_events: int,
    rng: random.Random,
    base_time: datetime,
) -> list[dict[str, Any]]:
    is_risky_user = rng.random() < 0.2
    events: list[dict[str, Any]] = []

    current_offset = rng.randint(0, 60)
    signup_ts = generate_timestamp(base_time, current_offset)
    events.append(generate_signup_event(user_id, signup_ts, rng, is_risky_user))

    for _ in range(1, num_events):
        current_offset += rng.randint(5, 30)
        ts = generate_timestamp(base_time, current_offset)

        event_type = rng.choices(
            ["login", "transaction"],
            weights=[0.4, 0.6],
            k=1,
        )[0]

        if event_type == "login":
            events.append(generate_login_event(user_id, ts, rng, is_risky_user))
        else:
            events.append(generate_transaction_event(user_id, ts, rng, is_risky_user))

    return events


def send_event(client: httpx.Client, api_url: str, event: dict[str, Any]) -> bool:
    try:
        response = client.post(f"{api_url}/events", json=event)
        return response.status_code in (200, 201, 202)
    except httpx.RequestError:
        return False


def print_separator() -> None:
    print("=" * 70)


def print_header(text: str) -> None:
    print_separator()
    print(f"  {text}")
    print_separator()


def run_demo(config: DemoConfig) -> dict[str, Any]:
    rng = random.Random(config.seed)
    base_time = datetime.now(UTC) - timedelta(hours=24)

    print_header("Risk Scoring Pipeline Demo")
    print("\nConfiguration:")
    print(f"  API URL:         {config.api_url}")
    print(f"  Users:           {config.num_users}")
    print(f"  Events/user:     {config.events_per_user}")
    print(f"  Random seed:     {config.seed}")
    print()

    all_events: list[dict[str, Any]] = []
    user_ids: list[str] = []

    print("Generating events...")
    for i in range(config.num_users):
        user_id = generate_user_id(i)
        user_ids.append(user_id)
        events = generate_user_event_sequence(
            user_id, config.events_per_user, rng, base_time
        )
        all_events.extend(events)

    total_events = len(all_events)
    print(f"Generated {total_events} events for {config.num_users} users\n")

    print_header("Sending Events to API")

    success_count = 0
    error_count = 0
    start_time = time.perf_counter()

    with httpx.Client(timeout=10.0) as client:
        for i, event in enumerate(all_events):
            if send_event(client, config.api_url, event):
                success_count += 1
            else:
                error_count += 1

            if (i + 1) % 10 == 0 or (i + 1) == total_events:
                print(f"  Progress: {i + 1}/{total_events} events sent", end="\r")

            if config.delay_ms > 0:
                time.sleep(config.delay_ms / 1000.0)

    elapsed = time.perf_counter() - start_time
    throughput = success_count / elapsed if elapsed > 0 else 0

    print("\n\nResults:")
    print(f"  Successful:   {success_count}")
    print(f"  Failed:       {error_count}")
    print(f"  Duration:     {elapsed:.2f}s")
    print(f"  Throughput:   {throughput:.1f} events/sec")
    print()

    print("Waiting for scoring worker to process events...")
    time.sleep(2)

    print_header("Example curl Commands")

    sample_users = user_ids[:3]
    sample_event = all_events[0] if all_events else None

    print("\n# Health check:")
    print(f"curl -s {config.api_url}/health | jq")

    print("\n# Get latest risk score for a user:")
    for user_id in sample_users:
        print(f"curl -s {config.api_url}/score/{user_id} | jq")

    print("\n# Get score history for a user:")
    if sample_users:
        print(f"curl -s '{config.api_url}/score/{sample_users[0]}/history?limit=5' | jq")

    print("\n# Submit a new event:")
    if sample_event:
        import json

        event_json = json.dumps(sample_event)
        print(f"curl -X POST {config.api_url}/events \\")
        print("  -H 'Content-Type: application/json' \\")
        print(f"  -d '{event_json}' | jq")

    print("\n# View DLQ entries:")
    print(f"curl -s '{config.api_url}/dlq?limit=10' | jq")

    print("\n# Get metrics (Prometheus format):")
    print(f"curl -s {config.api_url}/metrics")

    print()
    print_header("Sample Score Results")

    with httpx.Client(timeout=10.0) as client:
        for user_id in sample_users:
            try:
                response = client.get(f"{config.api_url}/score/{user_id}")
                if response.status_code == 200:
                    data = response.json()
                    print(f"\n  User: {user_id}")
                    print(f"    Score: {data.get('score', 'N/A'):.3f}")
                    print(f"    Band:  {data.get('band', 'N/A')}")
                    if data.get("top_features"):
                        print("    Top Features:")
                        for feature, value in list(data["top_features"].items())[:3]:
                            print(f"      - {feature}: {value:.4f}")
                elif response.status_code == 404:
                    print(f"\n  User: {user_id} - Not scored yet (worker may still be processing)")
            except httpx.RequestError as e:
                print(f"\n  User: {user_id} - Error fetching score: {e}")

    print()
    print_separator()
    print("  Demo complete!")
    print_separator()

    return {
        "users": config.num_users,
        "events_sent": success_count,
        "events_failed": error_count,
        "duration_seconds": elapsed,
        "throughput": throughput,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Demo script for Risk Scoring Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:8000",
        help="API base URL",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=5,
        help="Number of users to generate",
    )
    parser.add_argument(
        "--events-per-user",
        type=int,
        default=10,
        help="Number of events per user",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--delay-ms",
        type=int,
        default=0,
        help="Delay between events in milliseconds",
    )

    args = parser.parse_args()

    config = DemoConfig(
        api_url=args.api_url,
        num_users=args.users,
        events_per_user=args.events_per_user,
        seed=args.seed,
        delay_ms=args.delay_ms,
    )

    try:
        run_demo(config)
        return 0
    except httpx.ConnectError:
        print(f"\nError: Could not connect to API at {config.api_url}")
        print("Make sure the services are running: make up")
        return 1
    except KeyboardInterrupt:
        print("\n\nDemo interrupted.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
