"""
generate_data.py
Synthetic dataset generation for Milestone 4.
Generates 10M+ rows of ML feature data with seeded randomness
for full reproducibility.

Usage:
    python generate_data.py --rows 1000 --seed 42 --output data/
    python generate_data.py --rows 10000000 --seed 42 --output data/
"""

import argparse
import os
import time
import numpy as np
import pandas as pd
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic ML dataset")
    parser.add_argument("--rows",    type=int, default=10_000_000, help="Number of rows to generate")
    parser.add_argument("--seed",    type=int, default=42,         help="Random seed for reproducibility")
    parser.add_argument("--output",  type=str, default="data/",    help="Output directory")
    parser.add_argument("--partitions", type=int, default=10,      help="Number of output Parquet partitions")
    return parser.parse_args()


def generate_chunk(start_idx: int, chunk_size: int, seed: int) -> pd.DataFrame:
    """Generate a single chunk of synthetic data with a deterministic seed."""
    rng = np.random.default_rng(seed)

    user_ids     = np.arange(start_idx, start_idx + chunk_size, dtype=np.int64)
    age          = rng.integers(18, 90,  size=chunk_size).astype(np.int32)
    income       = rng.normal(55_000, 20_000, size=chunk_size).clip(10_000, 300_000)
    session_time = rng.exponential(scale=15.0, size=chunk_size).clip(0, 300)
    page_views   = rng.poisson(lam=8, size=chunk_size).astype(np.int32)
    clicks       = rng.poisson(lam=3, size=chunk_size).astype(np.int32)
    cart_value   = rng.lognormal(mean=3.5, sigma=1.2, size=chunk_size).clip(0, 5000)
    device_type  = rng.choice(["mobile", "desktop", "tablet"],
                               p=[0.55, 0.35, 0.10], size=chunk_size)
    region       = rng.choice(["north", "south", "east", "west", "central"],
                               p=[0.20, 0.25, 0.20, 0.20, 0.15], size=chunk_size)
    # Introduce ~5% missing values in income and cart_value for realism
    missing_mask_income = rng.random(size=chunk_size) < 0.05
    missing_mask_cart   = rng.random(size=chunk_size) < 0.05
    income_col     = income.astype(object)
    cart_value_col = cart_value.astype(object)
    income_col[missing_mask_income]     = None
    cart_value_col[missing_mask_cart]   = None

    # Binary label: purchase (1) or not (0)
    purchase_prob = (
        0.05
        + 0.0000003 * income.clip(0, 300_000)
        + 0.002     * page_views
        + 0.003     * clicks
    ).clip(0, 1)
    label = (rng.random(size=chunk_size) < purchase_prob).astype(np.int8)

    return pd.DataFrame({
        "user_id":      user_ids,
        "age":          age,
        "income":       income_col,
        "session_time": session_time.round(2),
        "page_views":   page_views,
        "clicks":       clicks,
        "cart_value":   cart_value_col,
        "device_type":  device_type,
        "region":       region,
        "label":        label,
    })


def main():
    args = parse_args()
    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"[generate_data] Generating {args.rows:,} rows | seed={args.seed} | partitions={args.partitions}")
    start = time.time()

    chunk_size  = max(1, args.rows // args.partitions)
    total_written = 0

    for part in range(args.partitions):
        part_seed   = args.seed + part          # deterministic per-partition seed
        start_idx   = part * chunk_size
        # last partition absorbs any remainder
        size        = chunk_size if part < args.partitions - 1 else args.rows - start_idx
        if size <= 0:
            break

        df = generate_chunk(start_idx, size, seed=part_seed)
        out_file = output_path / f"part-{part:04d}.parquet"
        df.to_parquet(out_file, index=False)
        total_written += len(df)
        print(f"  [partition {part+1:>3}/{args.partitions}] {len(df):>10,} rows → {out_file}")

    elapsed = time.time() - start
    print(f"\n[generate_data] Done: {total_written:,} rows in {elapsed:.2f}s")
    print(f"[generate_data] Output: {output_path.resolve()}")


if __name__ == "__main__":
    main()
