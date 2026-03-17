# IDS568 Milestone 4 — Distributed & Streaming Pipeline

**NetID:** tsriv  
**Course:** MLOps — Module 5  
**Framework:** PySpark 3.5.1 (distributed) + Python file-based queue (streaming)

---

## Repository Structure
```
ids568-milestone4-tsriv/
├── pipeline.py            # Distributed feature engineering (PySpark)
├── generate_data.py       # Synthetic dataset generation (10M+ rows)
├── producer.py            # Streaming event producer (file-based queue)
├── consumer.py            # Streaming consumer with tumbling windows
├── README.md              # This file
├── REPORT.md              # Performance analysis & architecture report
├── STREAMING_REPORT.md    # Streaming analysis & metrics
└── requirements.txt       # Python dependencies
```

---

## Environment Requirements

| Component | Version |
|-----------|---------|
| Python | 3.12 |
| Java | OpenJDK 11+ |
| PySpark | 3.5.1 |
| pyarrow | 23.0.1 |
| pandas | 3.0.1 |
| numpy | 2.4.3 |
| kafka-python | 2.3.0 |
| Faker | 40.11.0 |
| psutil | 7.2.2 |

---

## Quick Setup (Reproducing Results)

### 1. Clone the repository
```bash
git clone https://github.com/tsriv12/ids568-milestone4-tsriv.git
cd ids568-milestone4-tsriv
```

### 2. Create and activate virtual environment
```bash
python3 -m venv milestone4-env
source milestone4-env/bin/activate
```

### 3. Install dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Verify Java is installed (required for PySpark)
```bash
java -version
# Must show OpenJDK 11 or higher
```

---

## Part 1: Distributed Feature Engineering

### Step 1 — Generate synthetic dataset
```bash
# Small test (1K rows — quick sanity check)
python generate_data.py --rows 1000 --seed 42 --output test_data/

# Full benchmark dataset (10M rows)
python generate_data.py --rows 10000000 --seed 42 --partitions 20 --output data/
```

**Arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `--rows` | 10000000 | Number of rows to generate |
| `--seed` | 42 | Random seed (use 42 to reproduce published results) |
| `--output` | data/ | Output directory |
| `--partitions` | 10 | Number of Parquet output files |

### Step 2 — Run the pipeline
```bash
# Local mode (single worker — baseline)
python pipeline.py \
    --input data/ \
    --output output_local/ \
    --mode local \
    --partitions 4 \
    --workers 1

# Distributed mode (all available cores)
python pipeline.py \
    --input data/ \
    --output output_distributed/ \
    --mode distributed \
    --partitions 16
```

**Arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `--input` | data/ | Input Parquet directory |
| `--output` | output/ | Output directory |
| `--mode` | distributed | `local` or `distributed` |
| `--partitions` | 8 | Spark shuffle partitions |
| `--workers` | 4 | Threads for local mode only |

### Step 3 — View results
```bash
cat output_local/metrics.json
cat output_distributed/metrics.json
```

### Reproducibility Verification
```bash
python generate_data.py --rows 100 --seed 42 --output run1/
python generate_data.py --rows 100 --seed 42 --output run2/
diff -r run1/ run2/ && echo "REPRODUCIBLE" || echo "NOT REPRODUCIBLE"
rm -rf run1/ run2/
```

---

## Part 2: Streaming Pipeline (Bonus)

### Architecture

The streaming pipeline uses a **file-based mock queue** (per the assignment's
allowed alternatives to Kafka). The producer writes JSON event files to a
shared directory; the consumer polls that directory, processes events through
tumbling windows, and deletes files after consumption — mirroring Kafka
producer/consumer semantics without requiring a broker.
```
producer.py  →  queue_buffer/  →  consumer.py  →  streaming_results/
(event gen)     (shared dir)      (windowing)       (JSON outputs)
```

### Run producer and consumer together

Open two terminals (or use `&` for background):
```bash
# Terminal 1 — Start consumer first
python consumer.py \
    --queue-dir queue_buffer/ \
    --window 30 \
    --duration 90 \
    --output streaming_results/

# Terminal 2 — Start producer
python producer.py \
    --queue-dir queue_buffer/ \
    --rate 100 \
    --duration 60
```

**Producer arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `--queue-dir` | queue_buffer/ | Shared queue directory |
| `--rate` | 100 | Target events per second |
| `--duration` | 120 | Run duration in seconds |
| `--seed` | 42 | Random seed |
| `--load-test` | flag | Run automated load test (100/1000/5000 msg/s) |

**Consumer arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `--queue-dir` | queue_buffer/ | Shared queue directory (must match producer) |
| `--window` | 30 | Tumbling window size in seconds |
| `--max-lag` | 10 | Max seconds late events are still accepted |
| `--duration` | 180 | Consumer runtime in seconds |
| `--output` | streaming_results/ | Output directory for window results |
| `--load-test` | flag | Process all load test subdirs and report metrics |

### Run load test
```bash
# Step 1 — Run producer load test (generates data at 100, 1000, 5000 msg/s)
python producer.py --queue-dir queue_buffer/ --load-test

# Step 2 — Run consumer load test (processes and reports p50/p95/p99)
python consumer.py --queue-dir queue_buffer/ --load-test --output streaming_results/

# View results
cat streaming_results/load_test_consumer_results.json
```

### View window results
```bash
# Individual window outputs
ls streaming_results/window_*.json

# Overall summary
cat streaming_results/summary.json
```

---

## Automated Sanity Checks
```bash
# File existence
for file in pipeline.py generate_data.py README.md REPORT.md; do
    [ -f "$file" ] && echo "✓ $file" || echo "✗ $file MISSING"
done

# Python syntax
for file in *.py; do
    python -m py_compile "$file" && echo "✓ $file syntax OK" || echo "✗ $file errors"
done

# Pipeline smoke test
python generate_data.py --rows 1000 --seed 42 --output test_data/
python pipeline.py --input test_data/ --output test_output/ --mode local --partitions 4
echo "Exit code: $?"
rm -rf test_data/ test_output/
```

---

## Key Results Summary

| Metric | Local (1 worker) | Distributed (16 threads) |
|--------|-----------------|--------------------------|
| Runtime | 110.18s | 100.71s |
| Speedup | 1.0x | 1.09x |
| Partitions | 4 | 16 |
| Output size | 610 MB | 610 MB |
| Input rows | 10,000,000 | 10,000,000 |
| Output cols | 30 | 30 |

**Streaming (real-time, 100 msg/s):**

| Metric | Value |
|--------|-------|
| p50 latency | 25.53 ms |
| p95 latency | 48.26 ms |
| p99 latency | 50.21 ms |
| Breaking point | ~2,500 msg/s (disk saturation) |

See `REPORT.md` and `STREAMING_REPORT.md` for full analysis.

---

## Seeds & Determinism

All randomness is seeded. Use `--seed 42` (default) to reproduce
published results exactly.
