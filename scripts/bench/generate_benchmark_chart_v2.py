#!/usr/bin/env python3
"""
Generate a modern, eye-catching benchmark comparison chart for README.

Features:
- Gradient bars with glow effects
- Modern color palette
- Clean typography
- Speedup callouts
- Card-style layout

Usage:
    python scripts/bench/generate_benchmark_chart_v2.py

Output:
    docs/assets/benchmark-chart.png
    docs/assets/benchmark-chart-light.png
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
import numpy as np
from pathlib import Path

# Use a clean sans-serif font
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica', 'sans-serif']

# Benchmark data (from actual results)
SYSTEMS = ["timeseries-table", "ClickHouse", "Delta+Spark", "PostgreSQL", "TimescaleDB"]
DISPLAY_NAMES = ["timeseries-table", "ClickHouse", "Delta + Spark", "PostgreSQL", "TimescaleDB"]

# Modern gradient-friendly colors - softer, more muted palette
COLORS_DARK = {
    "timeseries-table": ("#E07A5F", "#E8998D"),  # Terracotta (warm but muted)
    "ClickHouse": ("#F2CC8F", "#F5DDB5"),        # Warm sand
    "Delta+Spark": ("#81B29A", "#A3C4B1"),       # Sage green  
    "PostgreSQL": ("#5B8FB9", "#89B4D4"),        # Soft blue
    "TimescaleDB": ("#9A8C98", "#B5AAB8"),       # Muted purple/mauve
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


def draw_rounded_bar(ax, x, y, width, height, color, alpha=1.0, glow=False):
    """Draw a rounded rectangle bar with optional glow effect."""
    if width <= 0:
        return
    
    # Glow effect for hero bar
    if glow:
        for i in range(3, 0, -1):
            glow_bar = FancyBboxPatch(
                (x, y - height/2), width, height,
                boxstyle=f"round,pad=0,rounding_size={height/3}",
                facecolor=color,
                edgecolor='none',
                alpha=0.15 * (4-i),
                transform=ax.transData,
                zorder=1
            )
            ax.add_patch(glow_bar)
    
    # Main bar
    bar = FancyBboxPatch(
        (x, y - height/2), width, height,
        boxstyle=f"round,pad=0,rounding_size={height/3}",
        facecolor=color,
        edgecolor='none',
        alpha=alpha,
        transform=ax.transData,
        zorder=2
    )
    ax.add_patch(bar)
    return bar


def create_modern_chart(dark_mode=True):
    """Create a modern benchmark chart."""
    
    # Colors based on mode
    if dark_mode:
        bg_color = "#0D1117"
        card_bg = "#161B22"
        text_color = "#E6EDF3"
        text_secondary = "#8B949E"
        grid_color = "#30363D"
    else:
        bg_color = "#FFFFFF"
        card_bg = "#F6F8FA"
        text_color = "#1F2328"
        text_secondary = "#656D76"
        grid_color = "#D0D7DE"
    
    fig = plt.figure(figsize=(14, 7), facecolor=bg_color)
    
    # Title area
    fig.text(0.5, 0.94, "⚡ Benchmark Results", fontsize=24, fontweight='bold',
             color=text_color, ha='center', va='top')
    fig.text(0.5, 0.89, "73M rows • NYC Taxi Dataset • Lower is better",
             fontsize=12, color=text_secondary, ha='center', va='top')
    
    operations = list(DATA.keys())
    
    # Create 3 card-style subplots
    for idx, op in enumerate(operations):
        # Position each "card"
        card_left = 0.05 + idx * 0.31
        card_width = 0.28
        card_bottom = 0.12
        card_height = 0.68
        
        ax = fig.add_axes([card_left, card_bottom, card_width, card_height])
        ax.set_facecolor(card_bg)
        
        # Add rounded card border
        for spine in ax.spines.values():
            spine.set_visible(False)
        
        # Card background with subtle border
        card_rect = FancyBboxPatch(
            (0, 0), 1, 1,
            boxstyle="round,pad=0.02,rounding_size=0.05",
            facecolor=card_bg,
            edgecolor=grid_color,
            linewidth=1,
            transform=ax.transAxes,
            zorder=0
        )
        ax.add_patch(card_rect)
        
        values = [DATA[op][sys] for sys in SYSTEMS]
        max_val = max(values)
        
        # Bar settings
        bar_height = 0.10
        bar_spacing = 0.16
        start_y = 0.78
        
        # Draw bars
        for i, (sys, val, name) in enumerate(zip(SYSTEMS, values, DISPLAY_NAMES)):
            y = start_y - i * bar_spacing
            bar_width = (val / max_val) * 0.60
            
            color = COLORS_DARK[sys][0]
            is_hero = (sys == "timeseries-table")
            
            draw_rounded_bar(ax, 0.02, y, bar_width, bar_height, color, 
                           alpha=1.0 if is_hero else 0.75,
                           glow=is_hero)
            
            # System name (left aligned, more space above bar)
            ax.text(0.02, y + bar_height/2 + 0.055, name, fontsize=9,
                   color=text_color, ha='left', va='bottom',
                   fontweight='bold' if is_hero else 'normal',
                   transform=ax.transAxes)
            
            # Time value (at end of bar or outside)
            time_str = format_time(val)
            if bar_width > 0.25:
                ax.text(0.02 + bar_width - 0.02, y, time_str, fontsize=10,
                       color='white', ha='right', va='center',
                       fontweight='bold', transform=ax.transAxes)
            else:
                ax.text(0.02 + bar_width + 0.02, y, time_str, fontsize=10,
                       color=text_color, ha='left', va='center',
                       fontweight='bold', transform=ax.transAxes)
            
            # Speedup badge for hero
            if is_hero and i == 0:
                speedup = values[1] / val  # vs second place
                badge_text = f"{speedup:.0f}× faster"
                ax.text(0.98, y, badge_text, fontsize=9,
                       color=COLORS_DARK["timeseries-table"][0], 
                       ha='right', va='center',
                       fontweight='bold', transform=ax.transAxes,
                       bbox=dict(boxstyle='round,pad=0.3', 
                                facecolor=COLORS_DARK["timeseries-table"][0],
                                alpha=0.15, edgecolor='none'))
        
        # Operation title at bottom
        ax.text(0.5, 0.02, op, fontsize=13, fontweight='bold',
               color=text_color, ha='center', va='bottom',
               transform=ax.transAxes)
        
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_xticks([])
        ax.set_yticks([])
    
    # Footer
    fig.text(0.5, 0.03, "See docs/benchmarks/ for full methodology and reproduction steps",
             fontsize=9, color=text_secondary, ha='center', style='italic')
    
    return fig


def main():
    output_dir = Path(__file__).parent.parent.parent / "docs" / "assets"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Dark mode version
    fig_dark = create_modern_chart(dark_mode=True)
    output_dark = output_dir / "benchmark-chart.png"
    fig_dark.savefig(output_dark, dpi=150, bbox_inches='tight',
                     facecolor='#0D1117', edgecolor='none', pad_inches=0.2)
    plt.close(fig_dark)
    print(f"✓ Saved dark mode: {output_dark}")
    
    # Light mode version
    fig_light = create_modern_chart(dark_mode=False)
    output_light = output_dir / "benchmark-chart-light.png"
    fig_light.savefig(output_light, dpi=150, bbox_inches='tight',
                      facecolor='white', edgecolor='none', pad_inches=0.2)
    plt.close(fig_light)
    print(f"✓ Saved light mode: {output_light}")


if __name__ == "__main__":
    main()
