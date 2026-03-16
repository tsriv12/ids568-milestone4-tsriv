"""
pipeline.py
Distributed Feature Engineering Pipeline using PySpark.

Implements feature engineering transformations on synthetic ML data,
with both local (single-partition) and distributed (multi-worker) modes
for performance comparison.

Usage:
    python pipeline.py --input data/ --output output/ --mode distributed
    python pipeline.py --input data/ --output output/ --mode local
"""

import argparse
import json
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="PySpark Feature Engineering Pipeline")
    parser.add_argument("--input",      type=str, default="data/",    help="Input directory (Parquet)")
    parser.add_argument("--output",     type=str, default="output/",  help="Output directory")
    parser.add_argument("--mode",       type=str, default="distributed",
                        choices=["local", "distributed"],              help="Execution mode")
    parser.add_argument("--partitions", type=int, default=8,          help="Number of Spark partitions")
    parser.add_argument("--workers",    type=int, default=4,          help="Worker threads (local mode)")
    return parser.parse_args()


# ── Spark session factory ─────────────────────────────────────────────────────

def create_spark_session(mode: str, workers: int, partitions: int) -> SparkSession:
    """Create a SparkSession configured for local or distributed mode."""
    master = f"local[{workers}]" if mode == "local" else f"local[*]"

    spark = (
        SparkSession.builder
        .master(master)
        .appName("Milestone4-FeatureEngineering")
        .config("spark.sql.shuffle.partitions",         str(partitions))
        .config("spark.default.parallelism",            str(partitions))
        .config("spark.sql.adaptive.enabled",           "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Enable Spark UI metrics collection
        .config("spark.eventLog.enabled",               "false")
        .config("spark.driver.memory",                  "4g")
        .config("spark.executor.memory",                "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Feature engineering transformations ──────────────────────────────────────

def engineer_features(df, partitions: int):
    """
    Apply distributed feature engineering transformations.

    Transformations:
    1.  Null imputation  — fill missing income / cart_value with column median proxy
    2.  Type casting     — ensure numeric columns are correct types
    3.  Derived features — click-through rate, cart-per-pageview, log income
    4.  Binning          — age groups, income brackets
    5.  One-hot encoding — device_type, region
    6.  Window features  — region-level avg session time & click rate
    7.  Normalization    — min-max scale session_time and page_views
    8.  Interaction term — income × page_views
    """

    # 1. Null imputation (fill with column mean via aggregation — fully distributed)
    income_mean    = df.select(F.mean("income")).collect()[0][0] or 55000.0
    cart_mean      = df.select(F.mean("cart_value")).collect()[0][0] or 50.0

    df = df.fillna({"income": income_mean, "cart_value": cart_mean})

    # 2. Type casting
    df = (
        df.withColumn("income",       F.col("income").cast(DoubleType()))
          .withColumn("cart_value",   F.col("cart_value").cast(DoubleType()))
          .withColumn("session_time", F.col("session_time").cast(DoubleType()))
          .withColumn("age",          F.col("age").cast(IntegerType()))
          .withColumn("page_views",   F.col("page_views").cast(IntegerType()))
          .withColumn("clicks",       F.col("clicks").cast(IntegerType()))
    )

    # 3. Derived features
    df = (
        df.withColumn("click_through_rate",
                      F.when(F.col("page_views") > 0,
                             F.col("clicks") / F.col("page_views")).otherwise(0.0))
          .withColumn("cart_per_pageview",
                      F.when(F.col("page_views") > 0,
                             F.col("cart_value") / F.col("page_views")).otherwise(0.0))
          .withColumn("log_income",
                      F.log1p(F.col("income")))
          .withColumn("log_cart_value",
                      F.log1p(F.col("cart_value")))
    )

    # 4. Binning
    df = (
        df.withColumn("age_group",
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

    # 5. One-hot encoding — device_type
    for device in ["mobile", "desktop", "tablet"]:
        df = df.withColumn(f"device_{device}",
                           (F.col("device_type") == device).cast(IntegerType()))

    # One-hot encoding — region
    for region in ["north", "south", "east", "west", "central"]:
        df = df.withColumn(f"region_{region}",
                           (F.col("region") == region).cast(IntegerType()))

    # 6. Window features — region-level aggregates (requires shuffle)
    region_window = Window.partitionBy("region")
    df = (
        df.withColumn("region_avg_session",
                      F.avg("session_time").over(region_window))
          .withColumn("region_avg_clicks",
                      F.avg("clicks").over(region_window))
          .withColumn("session_vs_region_avg",
                      F.col("session_time") - F.col("region_avg_session"))
    )

    # 7. Normalization — min-max (distributed aggregation then broadcast)
    stats = df.agg(
        F.min("session_time").alias("min_st"),
        F.max("session_time").alias("max_st"),
        F.min("page_views").alias("min_pv"),
        F.max("page_views").alias("max_pv"),
    ).collect()[0]

    st_range = float(stats["max_st"] - stats["min_st"]) or 1.0
    pv_range = float(stats["max_pv"] - stats["min_pv"]) or 1.0

    df = (
        df.withColumn("session_time_norm",
                      (F.col("session_time") - stats["min_st"]) / st_range)
          .withColumn("page_views_norm",
                      (F.col("page_views")   - stats["min_pv"]) / pv_range)
    )

    # 8. Interaction term
    df = df.withColumn("income_x_pageviews",
                       F.col("income") * F.col("page_views"))

    # Final repartition for balanced output
    df = df.repartition(partitions)

    return df


# ── Metrics collection ────────────────────────────────────────────────────────

def collect_metrics(spark: SparkSession, start_time: float, end_time: float,
                    input_count: int, output_count: int, mode: str,
                    partitions: int) -> dict:
    """Collect runtime and Spark metrics."""
    sc = spark.sparkContext
    status = sc.statusTracker()

    metrics = {
        "mode":              mode,
        "partitions":        partitions,
        "input_rows":        input_count,
        "output_rows":       output_count,
        "total_runtime_sec": round(end_time - start_time, 3),
        "spark_master":      sc.master,
        "spark_app_id":      sc.applicationId,
        "default_parallelism": sc.defaultParallelism,
    }
    return metrics


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)
    metrics_path = output_path / "metrics.json"

    print(f"\n{'='*60}")
    print(f"  Milestone 4 — Feature Engineering Pipeline")
    print(f"  Mode: {args.mode.upper()} | Partitions: {args.partitions}")
    print(f"{'='*60}\n")

    # Create Spark session
    spark = create_spark_session(args.mode, args.workers, args.partitions)
    print(f"[pipeline] Spark master : {spark.sparkContext.master}")
    print(f"[pipeline] Default parallelism: {spark.sparkContext.defaultParallelism}")

    # Read input data
    print(f"[pipeline] Reading data from: {args.input}")
    df_input = spark.read.parquet(args.input)
    df_input.cache()
    input_count = df_input.count()
    print(f"[pipeline] Input rows  : {input_count:,}")
    print(f"[pipeline] Input schema:")
    df_input.printSchema()

    # Run feature engineering
    print(f"\n[pipeline] Starting feature engineering...")
    t_start = time.time()
    df_output = engineer_features(df_input, args.partitions)

    # Write output (triggers full execution)
    out_dir = str(output_path / "features")
    df_output.write.mode("overwrite").parquet(out_dir)
    t_end = time.time()

    output_count = df_output.count()
    print(f"\n[pipeline] Output rows : {output_count:,}")
    print(f"[pipeline] Output cols : {len(df_output.columns)}")
    print(f"[pipeline] Runtime     : {t_end - t_start:.2f}s")
    print(f"[pipeline] Output path : {out_dir}")

    # Save metrics
    metrics = collect_metrics(spark, t_start, t_end,
                              input_count, output_count,
                              args.mode, args.partitions)
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\n[pipeline] Metrics saved → {metrics_path}")
    print(json.dumps(metrics, indent=2))

    spark.stop()
    print("\n[pipeline] Done ✓")


if __name__ == "__main__":
    main()
