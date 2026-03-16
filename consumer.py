"""
consumer.py
Streaming Consumer with Stateful Windowing using mock Python queue.

Reads events written by producer.py from queue_buffer/ directory.
Implements:
- Tumbling window aggregations
- p50/p95/p99 latency measurement
- Late-arriving data handling
- Crash recovery (resumes from last processed file)
- Backpressure detection via queue depth monitoring

Usage:
    python consumer.py --window 30 --duration 180
    python consumer.py --load-test
"""

import argparse
import json
import os
import glob
import signal
import statistics
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Mock Streaming Consumer")
    parser.add_argument("--queue-dir", type=str,  default="queue_buffer/",
                        help="Shared queue directory (matches producer)")
    parser.add_argument("--window",    type=int,  default=30,
                        help="Tumbling window size in seconds")
    parser.add_argument("--max-lag",   type=int,  default=10,
                        help="Max seconds late message is accepted")
    parser.add_argument("--duration",  type=int,  default=180,
                        help="Consumer runtime in seconds (0=forever)")
    parser.add_argument("--output",    type=str,  default="streaming_results/",
                        help="Output directory for window results")
    parser.add_argument("--load-test", action="store_true",
                        help="Process all load test subdirs and report metrics")
    return parser.parse_args()


# ── Tumbling window ───────────────────────────────────────────────────────────

class TumblingWindow:
    def __init__(self, window_sec: int, max_lag_sec: int):
        self.window_sec  = window_sec
        self.max_lag_sec = max_lag_sec
        self.buckets     = defaultdict(lambda: {
            "event_counts":  defaultdict(int),
            "user_ids":      set(),
            "cart_values":   [],
            "session_times": [],
            "page_views":    0,
            "clicks":        0,
            "latencies_ms":  [],
            "message_count": 0,
            "late_count":    0,
        })
        self.closed = []

    def _bucket(self, ts: float) -> int:
        return int(ts // self.window_sec) * self.window_sec

    def add(self, event: dict, receive_ts: float):
        try:
            event_ts = datetime.fromisoformat(
                event["timestamp"].replace("Z", "+00:00")).timestamp()
        except Exception:
            event_ts = receive_ts

        lag        = receive_ts - event_ts
        bk         = self._bucket(event_ts)
        now_bk     = self._bucket(receive_ts)

        # Drop events that are too late
        if bk < now_bk - self.window_sec and lag > self.max_lag_sec:
            self.buckets[bk]["late_count"] += 1
            return

        b = self.buckets[bk]
        b["event_counts"][event.get("event_type", "unknown")] += 1
        b["user_ids"].add(event.get("user_id"))
        b["latencies_ms"].append(lag * 1000)
        b["message_count"] += 1
        if event.get("cart_value", 0) > 0:
            b["cart_values"].append(event["cart_value"])
        if event.get("session_time"):
            b["session_times"].append(event["session_time"])
        b["page_views"] += event.get("page_views", 0)
        b["clicks"]     += event.get("clicks", 0)

        # Close expired buckets
        for k in [k for k in self.buckets if k < now_bk - self.window_sec]:
            self.closed.append(self._finalize(k))
            del self.buckets[k]

    def _finalize(self, bk: int) -> dict:
        b    = self.buckets[bk]
        lats = sorted(b["latencies_ms"])

        def pct(p):
            if not lats: return 0.0
            return round(lats[max(0, int(len(lats) * p / 100) - 1)], 3)

        return {
            "window_start":       datetime.fromtimestamp(bk, tz=timezone.utc).isoformat(),
            "window_end":         datetime.fromtimestamp(bk + self.window_sec, tz=timezone.utc).isoformat(),
            "message_count":      b["message_count"],
            "late_count":         b["late_count"],
            "unique_users":       len(b["user_ids"]),
            "event_counts":       dict(b["event_counts"]),
            "total_cart_value":   round(sum(b["cart_values"]), 2),
            "avg_session_time":   round(statistics.mean(b["session_times"]), 3)
                                  if b["session_times"] else 0.0,
            "click_through_rate": round(b["clicks"] / b["page_views"], 4)
                                  if b["page_views"] > 0 else 0.0,
            "latency_p50_ms":     pct(50),
            "latency_p95_ms":     pct(95),
            "latency_p99_ms":     pct(99),
            "latency_max_ms":     round(max(lats), 3) if lats else 0.0,
        }

    def drain(self):
        for k in list(self.buckets.keys()):
            self.closed.append(self._finalize(k))
            del self.buckets[k]


# ── Consumer ──────────────────────────────────────────────────────────────────

class StreamConsumer:
    def __init__(self, args):
        self.args         = args
        self.running      = True
        self.window       = TumblingWindow(args.window, args.max_lag)
        self.processed    = 0
        self.errors       = 0
        self.latencies    = deque(maxlen=50000)
        self.queue_depths = []
        self.start_time   = time.time()
        self.checkpoint   = Path(args.output) / ".checkpoint"
        Path(args.output).mkdir(parents=True, exist_ok=True)

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
        print(f"[consumer] Started | queue={args.queue_dir} | window={args.window}s")

    def _shutdown(self, *_):
        print("\n[consumer] Shutdown signal — draining windows...")
        self.running = False

    def _last_processed(self) -> str:
        """Crash recovery: read last successfully processed filename."""
        if self.checkpoint.exists():
            return self.checkpoint.read_text().strip()
        return ""

    def _save_checkpoint(self, fname: str):
        self.checkpoint.write_text(fname)

    def run(self):
        start       = time.time()
        last_report = start
        last_ckpt   = ""
        queue_dir   = Path(self.args.queue_dir)

        # Crash recovery: skip files already processed
        resume_from = self._last_processed()
        if resume_from:
            print(f"[consumer] Resuming from checkpoint: {resume_from}")

        print(f"[consumer] Consuming from {queue_dir} ...")

        try:
            while self.running:
                if self.args.duration > 0 and time.time() - start >= self.args.duration:
                    break

                # Get all event files, sorted for ordering
                files = sorted(glob.glob(str(queue_dir / "evt_*.json")))

                # Skip already-processed files (crash recovery)
                if resume_from:
                    files = [f for f in files if os.path.basename(f) > os.path.basename(resume_from)]
                    if files:
                        resume_from = ""   # caught up

                # Backpressure: track queue depth
                self.queue_depths.append(len(files))

                if not files:
                    time.sleep(0.05)
                    continue

                for fpath in files:
                    if not self.running:
                        break
                    try:
                        receive_ts = time.time()
                        with open(fpath) as f:
                            event = json.load(f)

                        self.window.add(event, receive_ts)

                        # Compute latency
                        event_ts   = datetime.fromisoformat(
                            event["timestamp"].replace("Z", "+00:00")).timestamp()
                        lat_ms     = (receive_ts - event_ts) * 1000
                        self.latencies.append(lat_ms)
                        self.processed += 1
                        last_ckpt = fpath

                        # Delete after processing (like a queue consumer)
                        os.remove(fpath)

                    except Exception as e:
                        self.errors += 1

                # Checkpoint every 1000 messages
                if last_ckpt and self.processed % 1000 == 0:
                    self._save_checkpoint(last_ckpt)

                # Flush closed windows
                self._flush_windows()

                # Progress report every 10s
                if time.time() - last_report >= 10:
                    self._report()
                    last_report = time.time()

        finally:
            self.window.drain()
            self._flush_windows()
            self._finalize()

    def _flush_windows(self):
        while self.window.closed:
            result = self.window.closed.pop(0)
            ts     = result["window_start"].replace(":", "-")[:19]
            fname  = Path(self.args.output) / f"window_{ts}.json"
            with open(fname, "w") as f:
                json.dump(result, f, indent=2)

    def _report(self):
        elapsed = time.time() - self.start_time
        lats    = sorted(self.latencies)
        n       = len(lats)
        def pct(p): return round(lats[max(0, int(n*p/100)-1)], 2) if lats else 0.0
        print(f"  [t={elapsed:>6.1f}s] processed={self.processed:>8,} "
              f"tput={self.processed/max(elapsed,0.001):>7.1f}/s "
              f"p50={pct(50):>7.2f}ms p95={pct(95):>7.2f}ms "
              f"p99={pct(99):>7.2f}ms q_depth={self.queue_depths[-1] if self.queue_depths else 0}")

    def _finalize(self):
        elapsed = time.time() - self.start_time
        lats    = sorted(self.latencies)
        n       = len(lats)
        def pct(p): return round(lats[max(0, int(n*p/100)-1)], 2) if lats else 0.0

        summary = {
            "total_processed":  self.processed,
            "total_errors":     self.errors,
            "elapsed_sec":      round(elapsed, 2),
            "throughput_per_s": round(self.processed / max(elapsed, 0.001), 1),
            "latency_p50_ms":   pct(50),
            "latency_p95_ms":   pct(95),
            "latency_p99_ms":   pct(99),
            "latency_max_ms":   round(max(lats), 2) if lats else 0.0,
            "queue_depth_max":  max(self.queue_depths, default=0),
            "queue_depth_avg":  round(sum(self.queue_depths) /
                                max(len(self.queue_depths), 1), 2),
        }
        print(f"\n[consumer] Final metrics:")
        print(json.dumps(summary, indent=2))

        out = Path(self.args.output) / "summary.json"
        with open(out, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"[consumer] Summary saved → {out}")
        return summary


# ── Load test consumer ────────────────────────────────────────────────────────

def run_load_test_consumer(base_queue_dir: str, window: int, output: str):
    """Process each load test subdirectory and collect metrics."""
    rates   = [100, 1000, 5000]
    results = {}

    print("\n" + "="*60)
    print("  LOAD TEST — Consumer Metrics")
    print("="*60)

    for rate in rates:
        qdir = Path(base_queue_dir) / f"load_{rate}"
        if not qdir.exists():
            print(f"[load-test] Skipping rate={rate} (no data found)")
            continue

        out_dir = Path(output) / f"load_{rate}"
        print(f"\n[load-test] Processing rate={rate}/s queue...")

        # Create a fresh consumer args-like object
        class Args:
            queue_dir = str(qdir)
            window    = 30
            max_lag   = 10
            duration  = 0      # run until queue is empty
            output    = str(out_dir)

        c = StreamConsumer(Args())
        # Run until queue drains (max 60s)
        c.args.duration = 60
        c.run()

        lats = sorted(c.latencies)
        n    = len(lats)
        def pct(p): return round(lats[max(0, int(n*p/100)-1)], 2) if lats else 0.0

        elapsed = time.time() - c.start_time
        results[str(rate)] = {
            "target_rate":    rate,
            "processed":      c.processed,
            "throughput":     round(c.processed / max(elapsed, 0.001), 1),
            "latency_p50_ms": pct(50),
            "latency_p95_ms": pct(95),
            "latency_p99_ms": pct(99),
            "queue_depth_max": max(c.queue_depths, default=0),
        }

    print("\n" + "="*60)
    print("  LOAD TEST CONSUMER RESULTS")
    print("="*60)
    print(f"{'Rate':>8} {'Processed':>10} {'Tput/s':>8} {'p50ms':>8} {'p95ms':>8} {'p99ms':>8} {'QDepth':>8}")
    print("-"*62)
    for rate, r in results.items():
        print(f"{rate:>8} {r['processed']:>10,} {r['throughput']:>8.1f} "
              f"{r['latency_p50_ms']:>8.2f} {r['latency_p95_ms']:>8.2f} "
              f"{r['latency_p99_ms']:>8.2f} {r['queue_depth_max']:>8}")

    out = Path(output) / "load_test_consumer_results.json"
    Path(output).mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n[load-test] Results saved → {out}")
    return results


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    if args.load_test:
        run_load_test_consumer(args.queue_dir, args.window, args.output)
    else:
        c = StreamConsumer(args)
        c.run()


if __name__ == "__main__":
    main()
