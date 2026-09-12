import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path


# ============================================================
# Configuration
# ============================================================

LATENCY_FILE = "latency.csv"
UTILIZATION_FILE = "utilization.csv"

OUTPUT_DIR = Path("graphs")
OUTPUT_DIR.mkdir(exist_ok=True)


# ============================================================
# Load data
# ============================================================

latency = pd.read_csv(LATENCY_FILE)
utilization = pd.read_csv(UTILIZATION_FILE)

latency["timestamp"] = pd.to_datetime(latency["timestamp"])
utilization["timestamp"] = pd.to_datetime(utilization["timestamp"])


# ============================================================
# 1. Response time over time
# ============================================================

plt.figure(figsize=(12, 6))

plt.plot(
    latency["timestamp"],
    latency["response_time_ms"],
    linewidth=1
)

plt.xlabel("Time")
plt.ylabel("Response Time (ms)")
plt.title("Response Time Over Time")
plt.grid(True, alpha=0.3)

plt.xticks(rotation=45)
plt.tight_layout()

plt.savefig(
    OUTPUT_DIR / "response_time_over_time.png",
    dpi=300
)

plt.close()


# ============================================================
# 2. CPU utilization over time
# ============================================================

plt.figure(figsize=(12, 6))

for system in range(1, 7):
    column = f"system{system}_cpu"

    if column in utilization.columns:
        plt.plot(
            utilization["timestamp"],
            utilization[column],
            label=f"System {system}"
        )

plt.xlabel("Time")
plt.ylabel("CPU Utilization (%)")
plt.title("CPU Utilization of Backend Systems")
plt.legend()
plt.grid(True, alpha=0.3)

plt.xticks(rotation=45)
plt.tight_layout()

plt.savefig(
    OUTPUT_DIR / "cpu_utilization_over_time.png",
    dpi=300
)

plt.close()


# ============================================================
# 3. Memory utilization over time
# ============================================================

plt.figure(figsize=(12, 6))

for system in range(1, 7):
    column = f"system{system}_memory"

    if column in utilization.columns:
        plt.plot(
            utilization["timestamp"],
            utilization[column],
            label=f"System {system}"
        )

plt.xlabel("Time")
plt.ylabel("Memory Utilization (%)")
plt.title("Memory Utilization of Backend Systems")
plt.legend()
plt.grid(True, alpha=0.3)

plt.xticks(rotation=45)
plt.tight_layout()

plt.savefig(
    OUTPUT_DIR / "memory_utilization_over_time.png",
    dpi=300
)

plt.close()


# ============================================================
# 4. Response-time distribution
# ============================================================

plt.figure(figsize=(10, 6))

plt.hist(
    latency["response_time_ms"],
    bins=50
)

plt.xlabel("Response Time (ms)")
plt.ylabel("Number of Requests")
plt.title("Response Time Distribution")
plt.grid(True, alpha=0.3)

plt.tight_layout()

plt.savefig(
    OUTPUT_DIR / "response_time_distribution.png",
    dpi=300
)

plt.close()


# ============================================================
# 5. P50 / P95 / P99 response time
# ============================================================

p50 = latency["response_time_ms"].quantile(0.50)
p95 = latency["response_time_ms"].quantile(0.95)
p99 = latency["response_time_ms"].quantile(0.99)

percentiles = {
    "P50": p50,
    "P95": p95,
    "P99": p99,
}

plt.figure(figsize=(8, 6))

plt.bar(
    percentiles.keys(),
    percentiles.values()
)

plt.xlabel("Percentile")
plt.ylabel("Response Time (ms)")
plt.title("Response Time Percentiles")

for i, value in enumerate(percentiles.values()):
    plt.text(
        i,
        value,
        f"{value:.1f} ms",
        ha="center",
        va="bottom"
    )

plt.grid(axis="y", alpha=0.3)
plt.tight_layout()

plt.savefig(
    OUTPUT_DIR / "response_time_percentiles.png",
    dpi=300
)

plt.close()


# ============================================================
# 6. Average CPU utilization
# ============================================================

cpu_columns = [
    f"system{i}_cpu"
    for i in range(1, 7)
    if f"system{i}_cpu" in utilization.columns
]

average_cpu = utilization[cpu_columns].mean()

plt.figure(figsize=(10, 6))

plt.bar(
    [c.replace("_cpu", "").replace("system", "System ")
     for c in cpu_columns],
    average_cpu.values
)

plt.xlabel("System")
plt.ylabel("Average CPU Utilization (%)")
plt.title("Average CPU Utilization")

plt.grid(axis="y", alpha=0.3)
plt.tight_layout()

plt.savefig(
    OUTPUT_DIR / "average_cpu_utilization.png",
    dpi=300
)

plt.close()


# ============================================================
# 7. Average memory utilization
# ============================================================

memory_columns = [
    f"system{i}_memory"
    for i in range(1, 7)
    if f"system{i}_memory" in utilization.columns
]

average_memory = utilization[memory_columns].mean()

plt.figure(figsize=(10, 6))

plt.bar(
    [c.replace("_memory", "").replace("system", "System ")
     for c in memory_columns],
    average_memory.values
)

plt.xlabel("System")
plt.ylabel("Average Memory Utilization (%)")
plt.title("Average Memory Utilization")

plt.grid(axis="y", alpha=0.3)
plt.tight_layout()

plt.savefig(
    OUTPUT_DIR / "average_memory_utilization.png",
    dpi=300
)

plt.close()


# ============================================================
# Print summary
# ============================================================

print()
print("=" * 60)
print("VISUALIZATION COMPLETE")
print("=" * 60)

print(f"Requests: {len(latency)}")
print(f"Average response time: {latency['response_time_ms'].mean():.2f} ms")
print(f"P50: {p50:.2f} ms")
print(f"P95: {p95:.2f} ms")
print(f"P99: {p99:.2f} ms")

print()
print("Average CPU:")
for column, value in average_cpu.items():
    print(f"  {column}: {value:.2f}%")

print()
print("Average memory:")
for column, value in average_memory.items():
    print(f"  {column}: {value:.2f}%")

print()
print(f"Graphs saved to: {OUTPUT_DIR}/")
