"""
producer.py
Streaming Event Producer using Python multiprocessing.Queue.

Generates realistic user-event streams at configurable rates,
simulating steady-state and bursty traffic patterns.
Writes events to a shared queue file (queue_buffer/) for the consumer.

Usage:
    python producer.py --rate 100 --duration 120
    python producer.py --load-test
"""

import argparse
import json
import os
import random
import time
import signal
from datetime import datetime, timezone
from pathlib import Path


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Mock Streaming Event Producer")
    parser.add_argument("--rate",      type=int,  default=100,
                        help="Events per second")
    parser.add_argument("--duration",  type=int,  default=120,
                        help="Run duration in seconds")
    parser.add_argument("--seed",      type=int,  default=42,
                        help="Random seed")
    parser.add_argument("--queue-dir", type=str,  default="queue_buffer/",
                        help="Shared queue directory")
    parser.add_argument("--load-test", action="store_true",
                        help="Run automated load test across multiple rates")
    return parser.parse_args()


# ── Event generation ──────────────────────────────────────────────────────────

DEVICE_TYPES   = ["mobile", "desktop", "tablet"]
REGIONS        = ["north", "south", "east", "west", "central"]
EVENT_TYPES    = ["page_view", "click", "add_to_cart", "purchase", "search"]
EVENT_WEIGHTS  = [0.45, 0.25, 0.15, 0.05, 0.10]


def generate_event(user_id: int, rng: random.Random) -> dict:
    event_type = rng.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    return {
        "event_id":    f"{user_id}-{int(time.time_ns())}",
        "user_id":     user_id,
        "event_type":  event_type,
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "session_time": round(rng.expovariate(1 / 15), 2),
        "page_views":  rng.randint(1, 20),
        "clicks":      rng.randint(0, 10),
        "cart_value":  round(rng.lognormvariate(3.5, 1.2), 2)
                       if event_type in ("add_to_cart", "purchase") else 0.0,
        "device_type": rng.choice(DEVICE_TYPES),
        "region":      rng.choice(REGIONS),
        "income":      round(rng.gauss(55000, 20000), 2),
    }


def rate_schedule(base_rate: int, elapsed: float, duration: float) -> int:
    """
    Realistic bursty traffic pattern:
    - 0–20%:  ramp up
    - 20–60%: steady state
    - 60–75%: 3× burst
    - 75–90%: steady state
    - 90–100%: cool-down
    """
    pct = elapsed / duration
    if pct < 0.20:
        return max(1, int(base_rate * (pct / 0.20)))
    elif pct < 0.60:
        return base_rate
    elif pct < 0.75:
        return base_rate * 3
    elif pct < 0.90:
        return base_rate
    else:
        return max(1, int(base_rate * ((1.0 - pct) / 0.10)))


# ── Producer ─────────────────────────────────────────────────────────────────

class EventProducer:
    def __init__(self, queue_dir: str, seed: int):
        self.queue_dir = Path(queue_dir)
        self.queue_dir.mkdir(parents=True, exist_ok=True)
        self.rng       = random.Random(seed)
        self.running   = True
        self.stats     = {"sent": 0, "errors": 0, "bytes": 0}
        # Sequential file counter for ordering
        self.file_idx  = 0
        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_):
        print("\n[producer] Shutting down...")
        self.running = False

    def _write_event(self, event: dict):
        """Write event as JSON line to a numbered file in queue_dir."""
        fname = self.queue_dir / f"evt_{self.file_idx:010d}.json"
        with open(fname, "w") as f:
            json.dump(event, f)
        self.file_idx += 1

    def run(self, rate: int, duration: int, use_schedule: bool = True) -> dict:
        print(f"[producer] Starting | rate={rate}/s | duration={duration}s | "
              f"queue={self.queue_dir}")
        start      = time.time()
        user_pool  = list(range(1, 100_001))
        next_tick  = start

        while self.running:
            elapsed = time.time() - start
            if elapsed >= duration:
                break

            current_rate = rate_schedule(rate, elapsed, duration) \
                           if use_schedule else rate
            interval = 1.0 / current_rate if current_rate > 0 else 1.0

            user_id = self.rng.choice(user_pool)
            event   = generate_event(user_id, self.rng)

            try:
                payload = json.dumps(event)
                self._write_event(event)
                self.stats["sent"]  += 1
                self.stats["bytes"] += len(payload)
            except Exception as e:
                self.stats["errors"] += 1

            # Progress every 5 seconds
            if self.stats["sent"] % max(1, rate * 5) == 0:
                actual = self.stats["sent"] / max(elapsed, 0.001)
                print(f"  [t={elapsed:>6.1f}s] sent={self.stats['sent']:>8,} "
                      f"actual={actual:>7.1f}/s burst={current_rate}/s")

            next_tick += interval
            sleep_time = next_tick - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

        elapsed = time.time() - start
        self.stats["elapsed"] = round(elapsed, 2)
        self.stats["avg_rate"] = round(self.stats["sent"] / max(elapsed, 0.001), 1)
        print(f"\n[producer] Done | sent={self.stats['sent']:,} | "
              f"avg={self.stats['avg_rate']}/s | elapsed={elapsed:.2f}s")
        return self.stats


# ── Load test ─────────────────────────────────────────────────────────────────

def run_load_test(queue_dir: str, seed: int):
    """Test at 100, 1000, 5000 msg/s for 30s each."""
    rates    = [100, 1000, 5000]
    duration = 30
    results  = {}

    print("\n" + "="*60)
    print("  LOAD TEST — Streaming Producer")
    print("="*60)

    for rate in rates:
        # Clean queue dir for each run
        qdir = Path(queue_dir) / f"load_{rate}"
        qdir.mkdir(parents=True, exist_ok=True)
        print(f"\n[load-test] Rate={rate}/s for {duration}s")
        p     = EventProducer(str(qdir), seed + rate)
        stats = p.run(rate=rate, duration=duration, use_schedule=False)
        results[str(rate)] = stats
        time.sleep(2)

    print("\n" + "="*60)
    print("  LOAD TEST RESULTS")
    print("="*60)
    print(f"{'Target':>8} {'Sent':>10} {'Actual/s':>10} {'Errors':>8}")
    print("-"*42)
    for rate, r in results.items():
        print(f"{rate:>8} {r['sent']:>10,} {r['avg_rate']:>10.1f} {r['errors']:>8}")

    out = Path(queue_dir) / "load_test_results.json"
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n[load-test] Results saved → {out}")
    return results


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    if args.load_test:
        run_load_test(args.queue_dir, args.seed)
    else:
        p = EventProducer(args.queue_dir, args.seed)
        p.run(rate=args.rate, duration=args.duration)


if __name__ == "__main__":
    main()
