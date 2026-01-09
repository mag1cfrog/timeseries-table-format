#!/usr/bin/env python3
"""
Generate benchmark comparison chart for README.

Usage:
    python scripts/bench/generate_benchmark_chart.py

Output:
    docs/assets/benchmark-chart.png
"""

import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Benchmark data (from actual results)
SYSTEMS = ["timeseries-table", "ClickHouse", "Delta+Spark", "PostgreSQL", "TimescaleDB"]
COLORS = {
    "timeseries-table": "#E6522C",  # Rust orange - hero color
    "ClickHouse": "#FFCC00",
    "Delta+Spark": "#00ADD8",
    "PostgreSQL": "#336791",
    "TimescaleDB": "#FDB515",
}

# Data in milliseconds
DATA = {
    "Bulk Ingest": {
        "timeseries-table": 1697,
        "ClickHouse": 13093,
        "Delta+Spark": 24196,
        "PostgreSQL": 45528,
        "TimescaleDB": 86555,
    },
    "Daily Append": {
        "timeseries-table": 335,
        "ClickHouse": 1114,
        "Delta+Spark": 1454,
        "PostgreSQL": 1829,
        "TimescaleDB": 3197,
    },
    "Time-Range Query": {
        "timeseries-table": 545,
        "ClickHouse": 1371,
        "Delta+Spark": 49722,
        "PostgreSQL": 43556,
        "TimescaleDB": 43923,
    },
}


def format_time(ms: float) -> str:
    """Format milliseconds to human readable string."""
    if ms >= 1000:
        return f"{ms/1000:.1f}s"
    return f"{int(ms)}ms"


def create_benchmark_chart():
    """Create a horizontal bar chart comparing benchmark results."""

    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    fig.patch.set_facecolor("#0d1117")  # GitHub dark background

    operations = list(DATA.keys())

    for idx, (ax, op) in enumerate(zip(axes, operations)):
        ax.set_facecolor("#0d1117")

        values = [DATA[op][sys] for sys in SYSTEMS]
        colors = [COLORS[sys] for sys in SYSTEMS]

        # Create horizontal bars
        y_pos = np.arange(len(SYSTEMS))
        bars = ax.barh(y_pos, values, color=colors, height=0.6, edgecolor="none")

        # Highlight the winner (timeseries-table)
        bars[0].set_edgecolor("#ffffff")
        bars[0].set_linewidth(2)

        # Add value labels
        for i, (bar, val) in enumerate(zip(bars, values)):
            # Position label inside or outside bar depending on size
            max_val = max(values)
            if val > max_val * 0.3:
                ax.text(
                    val - max_val * 0.02,
                    bar.get_y() + bar.get_height() / 2,
                    format_time(val),
                    va="center",
                    ha="right",
                    color="white",
                    fontweight="bold",
                    fontsize=10,
                )
            else:
                ax.text(
                    val + max_val * 0.02,
                    bar.get_y() + bar.get_height() / 2,
                    format_time(val),
                    va="center",
                    ha="left",
                    color="white",
                    fontweight="bold",
                    fontsize=10,
                )

        # Styling
        ax.set_yticks(y_pos)
        ax.set_yticklabels(SYSTEMS, fontsize=11, color="white")
        ax.set_title(op, fontsize=14, fontweight="bold", color="white", pad=10)
        ax.invert_yaxis()  # Top to bottom

        # Remove spines
        for spine in ax.spines.values():
            spine.set_visible(False)

        # Light grid
        ax.xaxis.grid(True, linestyle="--", alpha=0.2, color="white")
        ax.set_axisbelow(True)

        # Hide x-axis labels (values shown on bars)
        ax.set_xticks([])

        # Add speedup annotation for timeseries-table
        baseline = values[0]
        if idx == 0:  # Bulk ingest
            speedup_vs = "ClickHouse"
            speedup = values[1] / baseline
        else:
            speedup_vs = "others"
            speedup = np.mean(values[1:]) / baseline

    # Add title
    fig.suptitle(
        "Benchmark: 73M rows NYC Taxi Data",
        fontsize=16,
        fontweight="bold",
        color="white",
        y=1.02,
    )

    # Add subtitle with key insight
    fig.text(
        0.5,
        0.96,
        "Lower is better • timeseries-table highlighted",
        ha="center",
        fontsize=10,
        color="#8b949e",
        style="italic",
    )

    plt.tight_layout()

    # Save
    output_path = (
        Path(__file__).parent.parent.parent / "docs" / "assets" / "benchmark-chart.png"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plt.savefig(
        output_path,
        dpi=150,
        bbox_inches="tight",
        facecolor="#0d1117",
        edgecolor="none",
        pad_inches=0.3,
    )
    print(f"✓ Saved to {output_path}")

    # Also save a light version for light mode
    fig.patch.set_facecolor("white")
    for ax in axes:
        ax.set_facecolor("white")
        for label in ax.get_yticklabels():
            label.set_color("black")
        ax.title.set_color("black")
        ax.xaxis.grid(True, linestyle="--", alpha=0.3, color="gray")

    fig.texts[0].set_color("black")  # Title
    fig.texts[1].set_color("#666666")  # Subtitle

    # Update bar labels to black for light mode
    for ax in axes:
        for text in ax.texts:
            text.set_color("black")

    output_path_light = output_path.with_name("benchmark-chart-light.png")
    plt.savefig(
        output_path_light,
        dpi=150,
        bbox_inches="tight",
        facecolor="white",
        edgecolor="none",
        pad_inches=0.3,
    )
    print(f"✓ Saved to {output_path_light}")

    plt.close()


if __name__ == "__main__":
    create_benchmark_chart()
