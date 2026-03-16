# IDS568 Milestone 4 — Distributed & Streaming Pipeline

**NetID:** tsriv  
**Course:** MLOps — Module 5  
**Framework:** PySpark 3.5.1 + Kafka (streaming bonus)

---

## Repository Structure
```
ids568-milestone4-tsriv/
├── pipeline.py            # Distributed feature engineering (PySpark)
├── generate_data.py       # Synthetic dataset generation (10M+ rows)
├── producer.py            # Kafka streaming event producer
├── consumer.py            # Kafka streaming consumer with windowing
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
| kafka-python | 2.0.2+ |
| Kafka broker | 3.7.0 |

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
| `--seed` | 42 | Random seed (use 42 for reproducibility) |
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
| `--workers` | 4 | Threads for local mode |

### Step 3 — View results
```bash
# View performance metrics
cat output_local/metrics.json
cat output_distributed/metrics.json
```

### Reproducibility Verification
```bash
# Verify identical output across runs with same seed
python generate_data.py --rows 100 --seed 42 --output run1/
python generate_data.py --rows 100 --seed 42 --output run2/
diff -r run1/ run2/ && echo "REPRODUCIBLE" || echo "NOT REPRODUCIBLE"
rm -rf run1/ run2/
```

---

## Part 2: Streaming Pipeline (Bonus)

### Prerequisites — Start Kafka
```bash
# Start Zookeeper (terminal 1)
~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties &
sleep 5

# Start Kafka broker (terminal 2)
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties &
sleep 5

# Create topic
~/kafka/bin/kafka-topics.sh --create \
    --topic user-events \
    --bootstrap-server localhost:9092 \
    --partitions 4 \
    --replication-factor 1
```

### Run the streaming pipeline
```bash
# Terminal 1 — Start consumer first
python consumer.py --topic user-events --window 30

# Terminal 2 — Start producer
python producer.py --topic user-events --rate 100 --duration 120
```

### Run load test
```bash
# Automated load test across all rates
python producer.py --load-test --topic user-events
```

---

## Automated Sanity Checks

Run the full checklist verification:
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

See `REPORT.md` for full analysis.

---

## Seeds & Determinism

All randomness is seeded. Use `--seed 42` (default) to reproduce
published results exactly. Different seeds produce different data
but identical schema and statistical properties.
