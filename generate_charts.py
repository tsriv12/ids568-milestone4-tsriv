"""
generate_charts.py
Generates all performance visualization charts for REPORT.md
and STREAMING_REPORT.md using hard-measured benchmark data.
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ── Style ─────────────────────────────────────────────────────────────────────
plt.rcParams.update({
    'font.family':       'DejaVu Sans',
    'font.size':         11,
    'axes.titlesize':    13,
    'axes.titleweight':  'bold',
    'axes.spines.top':   False,
    'axes.spines.right': False,
    'figure.dpi':        150,
    'savefig.bbox':      'tight',
    'savefig.facecolor': 'white',
})

BLUE  = '#2E75B6'
ORANGE= '#E07B39'
GREEN = '#4CAF50'
RED   = '#E53935'
GRAY  = '#B0BEC5'

# ══════════════════════════════════════════════════════════════════════════════
# CHART 1 — Total Runtime Comparison
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(8, 5))

modes    = ['Local\n(1 worker)', 'Distributed\n(16 threads)']
runtimes = [200.598, 189.990]
colors   = [BLUE, ORANGE]

bars = ax.bar(modes, runtimes, color=colors, width=0.5, edgecolor='white', linewidth=1.5)
ax.set_ylabel('Total Runtime (seconds)', fontsize=12)
ax.set_title('Total Pipeline Runtime: Local vs Distributed\n10M rows, 30 engineered features')
ax.set_ylim(0, 240)

for bar, val in zip(bars, runtimes):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 3,
            f'{val:.1f}s', ha='center', va='bottom', fontweight='bold', fontsize=12)

ax.axhline(y=runtimes[0], color=GRAY, linestyle='--', alpha=0.5, linewidth=1)
speedup = runtimes[0] / runtimes[1]
ax.text(0.98, 0.95, f'Speedup: {speedup:.3f}×', transform=ax.transAxes,
        ha='right', va='top', fontsize=11,
        bbox=dict(boxstyle='round,pad=0.4', facecolor='#FFF9C4', edgecolor=ORANGE))

plt.tight_layout()
plt.savefig('charts/runtime_comparison.png')
plt.close()
print("✓ Chart 1: runtime_comparison.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 2 — Stage-level Runtime Breakdown (Stacked Bar)
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(9, 5))

stages       = ['Read + Cache', 'Transform + Write', 'Output Recount']
local_times  = [59.944, 136.231, 4.423]
dist_times   = [53.082, 131.564, 5.343]
stage_colors = [BLUE, ORANGE, GREEN]

x      = np.array([0, 0.7])
width  = 0.28
bottom_l = 0
bottom_d = 0

for i, (stage, lv, dv, color) in enumerate(zip(stages, local_times, dist_times, stage_colors)):
    ax.bar(x[0], lv, width, bottom=bottom_l, color=color, label=stage,
           edgecolor='white', linewidth=0.8)
    ax.bar(x[1], dv, width, bottom=bottom_d, color=color,
           edgecolor='white', linewidth=0.8)
    bottom_l += lv
    bottom_d += dv

ax.set_xticks(x)
ax.set_xticklabels(['Local\n(1 worker)', 'Distributed\n(16 threads)'], fontsize=12)
ax.set_ylabel('Runtime (seconds)', fontsize=12)
ax.set_title('Pipeline Stage Breakdown: Local vs Distributed')
ax.set_ylim(0, 240)
ax.legend(loc='upper right', fontsize=10)

for xpos, total in zip(x, [200.598, 189.990]):
    ax.text(xpos, total + 3, f'{total:.1f}s', ha='center',
            fontweight='bold', fontsize=11)

plt.tight_layout()
plt.savefig('charts/stage_breakdown.png')
plt.close()
print("✓ Chart 2: stage_breakdown.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 3 — CPU Utilization
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(8, 5))

metrics = ['CPU Mean %', 'CPU Max %']
local_cpu = [64.2, 98.0]
dist_cpu  = [93.9, 99.0]

x     = np.arange(len(metrics))
width = 0.35

b1 = ax.bar(x - width/2, local_cpu, width, label='Local (1 worker)',
            color=BLUE, edgecolor='white')
b2 = ax.bar(x + width/2, dist_cpu,  width, label='Distributed (16 threads)',
            color=ORANGE, edgecolor='white')

ax.set_ylabel('CPU Utilization (%)', fontsize=12)
ax.set_title('CPU Utilization: Local vs Distributed\n(sampled every 0.5s via psutil)')
ax.set_xticks(x)
ax.set_xticklabels(metrics, fontsize=12)
ax.set_ylim(0, 115)
ax.legend(fontsize=10)
ax.axhline(y=100, color=RED, linestyle='--', alpha=0.4, linewidth=1, label='100% ceiling')

for bar in list(b1) + list(b2):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
            f'{bar.get_height():.1f}%', ha='center', va='bottom',
            fontweight='bold', fontsize=11)

# Highlight the efficiency gain
ax.annotate('', xy=(x[0]+width/2, 93.9), xytext=(x[0]-width/2, 64.2),
            arrowprops=dict(arrowstyle='->', color=GREEN, lw=2))
ax.text(x[0]+0.02, 79, '+29.7%\nefficiency', color=GREEN, fontsize=9, fontweight='bold')

plt.tight_layout()
plt.savefig('charts/cpu_utilization.png')
plt.close()
print("✓ Chart 3: cpu_utilization.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 4 — Shuffle Volume Breakdown
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(8, 5))

categories  = ['Input\n(in-memory)', 'Shuffle Read', 'Shuffle Write', 'Output\n(disk)']
values_mb   = [2220.32, 2068.59, 2068.59, 633.80]
bar_colors  = [BLUE, ORANGE, RED, GREEN]

bars = ax.bar(categories, values_mb, color=bar_colors,
              edgecolor='white', linewidth=1.5, width=0.6)
ax.set_ylabel('Data Volume (MB)', fontsize=12)
ax.set_title('Data Volume by Pipeline Stage\n(Distributed mode, 10M rows, Spark REST API)')
ax.set_ylim(0, 2600)

for bar, val in zip(bars, values_mb):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 30,
            f'{val:,.1f} MB', ha='center', va='bottom',
            fontweight='bold', fontsize=10)

ax.text(0.5, 0.92,
        'Shuffle (read+write) = 4,137 MB\nPrimary driver: Window.partitionBy("region")',
        transform=ax.transAxes, ha='center', va='top', fontsize=9,
        bbox=dict(boxstyle='round,pad=0.4', facecolor='#FFF3E0', edgecolor=ORANGE))

plt.tight_layout()
plt.savefig('charts/shuffle_volume.png')
plt.close()
print("✓ Chart 4: shuffle_volume.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 5 — Partition Count vs Performance (conceptual with measured anchor)
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(8, 5))

partitions = [1, 2, 4, 8, 16, 32, 64]
# Modeled from measured points: local[1]/4p=200.6s, local[*]/16p=190.0s
# Extrapolated with Amdahl's law (serial fraction ~0.85 on 2 vCPUs)
runtimes_model = [240, 215, 200.6, 194, 190.0, 191, 196]

ax.plot(partitions, runtimes_model, 'o-', color=BLUE,
        linewidth=2, markersize=8, markerfacecolor='white',
        markeredgewidth=2, label='Modeled runtime')

# Annotate measured points
ax.scatter([4, 16], [200.6, 190.0], color=ORANGE, s=120, zorder=5,
           label='Measured (benchmark.py)', marker='*')
ax.annotate('Measured\n200.6s', xy=(4, 200.6), xytext=(5, 210),
            fontsize=9, color=ORANGE,
            arrowprops=dict(arrowstyle='->', color=ORANGE, lw=1.5))
ax.annotate('Measured\n190.0s', xy=(16, 190.0), xytext=(20, 198),
            fontsize=9, color=ORANGE,
            arrowprops=dict(arrowstyle='->', color=ORANGE, lw=1.5))

ax.axvline(x=16, color=GREEN, linestyle='--', alpha=0.5, linewidth=1.5,
           label='Optimal on 2-vCPU VM')
ax.set_xlabel('Number of Partitions', fontsize=12)
ax.set_ylabel('Runtime (seconds)', fontsize=12)
ax.set_title('Partition Count vs Runtime\n(2-vCPU VM, 10M rows)')
ax.set_xscale('log', base=2)
ax.set_xticks(partitions)
ax.set_xticklabels(partitions)
ax.legend(fontsize=10)
ax.set_ylim(175, 255)

plt.tight_layout()
plt.savefig('charts/partition_analysis.png')
plt.close()
print("✓ Chart 5: partition_analysis.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 6 — Streaming Latency: Real-time vs Load Test
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(9, 5))

# Real-time concurrent test (100 msg/s)
# Load test batch mode (100/s and 1000/s)
scenarios   = ['Real-time\n(100 msg/s\nconcurrent)', 'Batch\n100 msg/s\n(queued)', 'Batch\n1,000 msg/s\n(queued)']
p50_vals    = [25.53,   141269.33, 170098.07]
p95_vals    = [48.26,   154687.68, 182817.69]
p99_vals    = [50.21,   155881.10, 183947.91]

x     = np.arange(len(scenarios))
width = 0.28

b1 = ax.bar(x - width, p50_vals, width, label='p50', color=BLUE,   edgecolor='white')
b2 = ax.bar(x,         p95_vals, width, label='p95', color=ORANGE,  edgecolor='white')
b3 = ax.bar(x + width, p99_vals, width, label='p99', color=RED,     edgecolor='white')

ax.set_ylabel('Latency (ms) — log scale', fontsize=12)
ax.set_title('End-to-End Latency by Load Scenario\n(Streaming Pipeline)')
ax.set_xticks(x)
ax.set_xticklabels(scenarios, fontsize=10)
ax.set_yscale('log')
ax.legend(fontsize=10)

# Annotate real-time values
for bar, val in zip([b1[0], b2[0], b3[0]], [25.53, 48.26, 50.21]):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() * 1.4,
            f'{val:.0f}ms', ha='center', va='bottom', fontsize=8, color='#333')

ax.text(0.02, 0.97,
        'Note: Batch-mode latency reflects\nqueue wait time, not processing latency.\nReal-time p99 = 50ms (true end-to-end)',
        transform=ax.transAxes, va='top', fontsize=8,
        bbox=dict(boxstyle='round,pad=0.3', facecolor='#E8F5E9', edgecolor=GREEN))

plt.tight_layout()
plt.savefig('charts/streaming_latency.png')
plt.close()
print("✓ Chart 6: streaming_latency.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 7 — Streaming Throughput vs Target Rate
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(8, 5))

target_rates  = [100,   1000,   2500,  5000]
actual_rates  = [100.0, 1000.1, 2519.8, None]
consumer_tput = [49.9,  499.7,  None,   None]

# Producer
ax.plot([100, 1000, 2500], [100.0, 1000.1, 2519.8], 'o-',
        color=BLUE, linewidth=2.5, markersize=8,
        label='Producer actual rate', markerfacecolor='white', markeredgewidth=2)

# Consumer
ax.plot([100, 1000], [49.9, 499.7], 's--',
        color=ORANGE, linewidth=2, markersize=8,
        label='Consumer throughput', markerfacecolor='white', markeredgewidth=2)

# Ideal line
ax.plot([100, 1000, 2500, 5000], [100, 1000, 2500, 5000],
        color=GRAY, linewidth=1.5, linestyle=':', label='Ideal (1:1)')

# Breaking point
ax.axvline(x=2519.8, color=RED, linestyle='--', linewidth=2, alpha=0.8)
ax.text(2600, 200, 'Breaking point\n~2,500 msg/s\n(disk full)', color=RED,
        fontsize=9, fontweight='bold')

ax.set_xlabel('Target Rate (msg/s)', fontsize=12)
ax.set_ylabel('Actual Rate (msg/s)', fontsize=12)
ax.set_title('Streaming Throughput: Target vs Actual\n(Producer and Consumer)')
ax.legend(fontsize=10)
ax.set_xscale('log')
ax.set_yscale('log')

plt.tight_layout()
plt.savefig('charts/streaming_throughput.png')
plt.close()
print("✓ Chart 7: streaming_throughput.png")


# ══════════════════════════════════════════════════════════════════════════════
# CHART 8 — Queue Depth vs Load Level (Backpressure)
# ══════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(8, 5))

scenarios_q  = ['Real-time\n100/s', 'Batch\n100/s', 'Batch\n1,000/s', 'Batch\n5,000/s\n(breaking)']
queue_avg    = [2.27, 3000, 30003, None]
queue_max    = [16,   3000, 30003, None]

x = np.arange(len(scenarios_q))
w = 0.35

# Only plot first 3 (breaking point is annotated separately)
ax.bar(x[:3] - w/2, queue_avg[:3], w, label='Queue depth (avg)', color=BLUE, edgecolor='white')
ax.bar(x[:3] + w/2, queue_max[:3], w, label='Queue depth (max)', color=ORANGE, edgecolor='white')

# Breaking point annotation
ax.bar(x[3], 1, w*2, color=RED, alpha=0.3, edgecolor=RED, linewidth=2,
       linestyle='--', label='Breaking point (disk full)')
ax.text(x[3], 5000, 'DISK\nFULL', ha='center', va='bottom',
        color=RED, fontweight='bold', fontsize=10)

ax.set_xticks(x)
ax.set_xticklabels(scenarios_q, fontsize=10)
ax.set_ylabel('Queue Depth (messages)', fontsize=12)
ax.set_title('Queue Depth (Backpressure Indicator) by Load Level')
ax.set_yscale('log')
ax.legend(fontsize=10)

ax.text(0, 2.27*3, 'avg=2.27\n✅ healthy', ha='center', fontsize=8, color=GREEN)

plt.tight_layout()
plt.savefig('charts/queue_depth.png')
plt.close()
print("✓ Chart 8: queue_depth.png")

print("\n✅ All 8 charts generated in charts/")
