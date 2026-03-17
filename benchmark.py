"""
benchmark.py
Instrumented benchmark runner for Milestone 4.
Captures hard measurements of:
- Runtime per stage
- Shuffle read/write bytes (from Spark listener)
- Peak memory (psutil sampling thread)
- CPU utilization (psutil sampling thread)
- Input/output disk sizes
Writes full metrics to benchmark_results/metrics_<mode>.json
"""

import argparse
import json
import os
import threading
import time
from pathlib import Path

import psutil
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",      default="data/")
    parser.add_argument("--output",     default="benchmark_results/")
    parser.add_argument("--mode",       default="distributed",
                        choices=["local", "distributed"])
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--workers",    type=int, default=1)
    return parser.parse_args()


# ── Resource sampler ──────────────────────────────────────────────────────────

class ResourceSampler:
    """Background thread sampling CPU and memory every 0.5s."""

    def __init__(self):
        self.cpu_samples    = []
        self.mem_samples    = []
        self.running        = False
        self._thread        = None
        self.proc           = psutil.Process(os.getpid())

    def start(self):
        self.running = True
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._thread.start()

    def stop(self):
        self.running = False
        if self._thread:
            self._thread.join(timeout=2)

    def _sample(self):
        while self.running:
            try:
                # System-wide CPU %
                self.cpu_samples.append(psutil.cpu_percent(interval=None))
                # RSS memory of this process in bytes
                self.mem_samples.append(self.proc.memory_info().rss)
            except Exception:
                pass
            time.sleep(0.5)

    def summary(self) -> dict:
        cpu = self.cpu_samples
        mem = self.mem_samples
        return {
            "cpu_mean_pct":    round(sum(cpu) / len(cpu), 1) if cpu else 0,
            "cpu_max_pct":     round(max(cpu), 1) if cpu else 0,
            "cpu_min_pct":     round(min(cpu), 1) if cpu else 0,
            "mem_peak_gb":     round(max(mem) / 1e9, 3) if mem else 0,
            "mem_mean_gb":     round(sum(mem) / len(mem) / 1e9, 3) if mem else 0,
            "sample_count":    len(cpu),
        }


# ── Spark metrics via AccumulatorV2 / REST ────────────────────────────────────

def get_spark_metrics(spark: SparkSession) -> dict:
    """Extract shuffle metrics from Spark REST API (localhost:4040)."""
    import urllib.request, urllib.error
    try:
        app_id = spark.sparkContext.applicationId
        url = f"http://localhost:4040/api/v1/applications/{app_id}/stages"
        with urllib.request.urlopen(url, timeout=3) as r:
            stages = json.loads(r.read().decode())

        shuffle_read  = sum(s.get("shuffleReadBytes",  0) for s in stages)
        shuffle_write = sum(s.get("shuffleWriteBytes", 0) for s in stages)
        input_bytes   = sum(s.get("inputBytes",        0) for s in stages)
        output_bytes  = sum(s.get("outputBytes",       0) for s in stages)
        total_tasks   = sum(s.get("numTasks",          0) for s in stages)
        failed_tasks  = sum(s.get("numFailedTasks",    0) for s in stages)

        return {
            "shuffle_read_bytes":   shuffle_read,
            "shuffle_read_mb":      round(shuffle_read  / 1e6, 2),
            "shuffle_write_bytes":  shuffle_write,
            "shuffle_write_mb":     round(shuffle_write / 1e6, 2),
            "input_bytes":          input_bytes,
            "input_mb":             round(input_bytes   / 1e6, 2),
            "output_bytes":         output_bytes,
            "output_mb":            round(output_bytes  / 1e6, 2),
            "total_tasks":          total_tasks,
            "failed_tasks":         failed_tasks,
            "stage_count":          len(stages),
            "source": "spark_rest_api"
        }
    except Exception as e:
        # Fallback: use file sizes as proxy
        return {"source": "fallback_file_sizes", "error": str(e)}


# ── Pipeline (same as pipeline.py) ───────────────────────────────────────────

def engineer_features(df, partitions):
    income_mean = df.select(F.mean("income")).collect()[0][0] or 55000.0
    cart_mean   = df.select(F.mean("cart_value")).collect()[0][0] or 50.0
    df = df.fillna({"income": income_mean, "cart_value": cart_mean})
    df = (df
        .withColumn("income",       F.col("income").cast(DoubleType()))
        .withColumn("cart_value",   F.col("cart_value").cast(DoubleType()))
        .withColumn("session_time", F.col("session_time").cast(DoubleType()))
        .withColumn("age",          F.col("age").cast(IntegerType()))
        .withColumn("page_views",   F.col("page_views").cast(IntegerType()))
        .withColumn("clicks",       F.col("clicks").cast(IntegerType()))
        .withColumn("click_through_rate",
            F.when(F.col("page_views") > 0,
                   F.col("clicks") / F.col("page_views")).otherwise(0.0))
        .withColumn("cart_per_pageview",
            F.when(F.col("page_views") > 0,
                   F.col("cart_value") / F.col("page_views")).otherwise(0.0))
        .withColumn("log_income",     F.log1p(F.col("income")))
        .withColumn("log_cart_value", F.log1p(F.col("cart_value")))
        .withColumn("age_group",
            F.when(F.col("age") < 25, "youth")
             .when(F.col("age") < 40, "young_adult")
             .when(F.col("age") < 60, "middle_aged")
             .otherwise("senior"))
        .withColumn("income_bracket",
            F.when(F.col("income") < 30000,  "low")
             .when(F.col("income") < 70000,  "medium")
             .when(F.col("income") < 150000, "high")
             .otherwise("very_high"))
    )
    for d in ["mobile", "desktop", "tablet"]:
        df = df.withColumn(f"device_{d}", (F.col("device_type") == d).cast(IntegerType()))
    for r in ["north", "south", "east", "west", "central"]:
        df = df.withColumn(f"region_{r}", (F.col("region") == r).cast(IntegerType()))

    rw = Window.partitionBy("region")
    df = (df
        .withColumn("region_avg_session",    F.avg("session_time").over(rw))
        .withColumn("region_avg_clicks",     F.avg("clicks").over(rw))
        .withColumn("session_vs_region_avg", F.col("session_time") - F.col("region_avg_session"))
    )
    stats = df.agg(
        F.min("session_time").alias("min_st"), F.max("session_time").alias("max_st"),
        F.min("page_views").alias("min_pv"),   F.max("page_views").alias("max_pv"),
    ).collect()[0]
    st_range = float(stats["max_st"] - stats["min_st"]) or 1.0
    pv_range = float(stats["max_pv"] - stats["min_pv"]) or 1.0
    df = (df
        .withColumn("session_time_norm",
            (F.col("session_time") - stats["min_st"]) / st_range)
        .withColumn("page_views_norm",
            (F.col("page_views")   - stats["min_pv"]) / pv_range)
        .withColumn("income_x_pageviews", F.col("income") * F.col("page_views"))
    )
    return df.repartition(partitions)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    out  = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    master = f"local[{args.workers}]" if args.mode == "local" else "local[*]"

    spark = (SparkSession.builder
        .master(master)
        .appName(f"Benchmark-{args.mode}")
        .config("spark.sql.shuffle.partitions",          str(args.partitions))
        .config("spark.default.parallelism",             str(args.partitions))
        .config("spark.sql.adaptive.enabled",            "true")
        .config("spark.eventLog.enabled",                "false")
        .config("spark.driver.memory",                   "4g")
        .config("spark.executor.memory",                 "4g")
        .config("spark.ui.enabled",                      "true")
        .config("spark.ui.port",                         "4040")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"  INSTRUMENTED BENCHMARK — {args.mode.upper()}")
    print(f"  master={master} | partitions={args.partitions}")
    print(f"{'='*60}\n")

    # Measure input size on disk
    input_disk_bytes = sum(
        f.stat().st_size for f in Path(args.input).glob("*.parquet")
    )

    # Start resource sampler
    sampler = ResourceSampler()
    sampler.start()

    # --- TIMED SECTION START ---
    t0 = time.time()

    df = spark.read.parquet(args.input)
    df.cache()
    input_rows = df.count()

    t_read = time.time()

    df_out = engineer_features(df, args.partitions)
    feat_out = str(out / f"features_{args.mode}")
    df_out.write.mode("overwrite").parquet(feat_out)

    t_write = time.time()
    output_rows = df_out.count()

    t_end = time.time()
    # --- TIMED SECTION END ---

    sampler.stop()
    resource = sampler.summary()

    # Measure output size on disk
    output_disk_bytes = sum(
        f.stat().st_size for f in Path(feat_out).glob("*.parquet")
    )

    # Get shuffle metrics from Spark REST API
    spark_metrics = get_spark_metrics(spark)

    # Stage timing breakdown
    timing = {
        "read_cache_sec":    round(t_read  - t0,      3),
        "transform_write_sec": round(t_write - t_read, 3),
        "recount_sec":       round(t_end   - t_write, 3),
        "total_sec":         round(t_end   - t0,      3),
    }

    results = {
        "mode":              args.mode,
        "spark_master":      spark.sparkContext.master,
        "default_parallelism": spark.sparkContext.defaultParallelism,
        "partitions_config": args.partitions,
        "input_rows":        input_rows,
        "output_rows":       output_rows,
        "input_cols":        10,
        "output_cols":       len(df_out.columns),
        "input_disk_mb":     round(input_disk_bytes  / 1e6, 2),
        "output_disk_mb":    round(output_disk_bytes / 1e6, 2),
        "timing":            timing,
        "resource_usage":    resource,
        "spark_metrics":     spark_metrics,
    }

    print(json.dumps(results, indent=2))

    out_file = out / f"metrics_{args.mode}.json"
    with open(out_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n[benchmark] Saved → {out_file}")

    spark.stop()


if __name__ == "__main__":
    main()
