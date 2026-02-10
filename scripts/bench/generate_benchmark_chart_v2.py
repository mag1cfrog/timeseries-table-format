#!/usr/bin/env python3
"""
Generate a modern, eye-catching benchmark comparison chart for README.

Usage:
    python scripts/bench/generate_benchmark_chart_v2.py

Output:
    docs/assets/benchmark-chart.png
    docs/assets/benchmark-chart-light.png
"""

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
from pathlib import Path

# =============================================================================
# CONFIGURATION - Adjust these values to customize the chart
# =============================================================================

# --- Figure Settings ---
FIG_WIDTH = 14  # Total figure width in inches
FIG_HEIGHT = 7  # Total figure height in inches
FIG_DPI = 150  # Output resolution

# --- Title Settings ---
TITLE_TEXT = "⚡ Benchmark Results"
TITLE_FONTSIZE = 24
TITLE_Y = 0.94  # Vertical position (0-1)

SUBTITLE_TEXT = "73M rows • NYC Taxi Dataset • Lower is better"
SUBTITLE_FONTSIZE = 12
SUBTITLE_Y = 0.89

# --- Card (Panel) Settings ---
CARD_LEFT_START = 0.05  # Left margin for first card
CARD_SPACING = 0.31  # Horizontal spacing between cards
CARD_WIDTH = 0.28  # Width of each card
CARD_BOTTOM = 0.12  # Bottom margin
CARD_HEIGHT = 0.68  # Height of each card
CARD_ROUNDING = 0.05  # Corner rounding for cards
CARD_BORDER_WIDTH = 1  # Border line width

# --- Bar Settings ---
BAR_HEIGHT = 0.06  # Height of each bar (in axes coords)
BAR_SPACING = 0.16  # Vertical spacing between bars
BAR_START_Y = 0.78  # Y position of first bar
BAR_LEFT_MARGIN = 0.02  # Left margin for bars
BAR_MAX_WIDTH = 0.90  # Maximum bar width (for longest bar)
BAR_ROUNDING_FACTOR = 3  # Bar corner rounding (height / this value)
BAR_HERO_ALPHA = 1.0  # Opacity for hero (winner) bar
BAR_OTHER_ALPHA = 0.75  # Opacity for other bars

# --- Label Settings ---
LABEL_FONTSIZE = 9  # Font size for system names
LABEL_OFFSET_Y = 0.01  # Vertical offset above bar
TIME_FONTSIZE = 10  # Font size for time values
TIME_INSIDE_THRESHOLD = 0.25  # Bar width threshold for inside/outside label
TIME_INSIDE_OFFSET = 0.02  # Offset from bar end (inside)
TIME_OUTSIDE_OFFSET = 0.02  # Offset from bar end (outside)

# --- Speedup Badge Settings ---
BADGE_FONTSIZE = 9
BADGE_X = 0.98  # X position (right side of card)
BADGE_PADDING = 0.3  # Padding inside badge
BADGE_ALPHA = 0.15  # Badge background opacity

# --- Operation Title Settings ---
OP_TITLE_FONTSIZE = 13
OP_TITLE_Y = 0.02  # Y position at bottom of card

# --- Footer Settings ---
FOOTER_TEXT = (
    "See docs/benchmarks/README.md for full methodology and reproduction steps"
)
FOOTER_FONTSIZE = 9
FOOTER_Y = 0.03

# --- Glow Effect Settings ---
GLOW_ENABLED = True  # Enable/disable glow on hero bar
GLOW_LAYERS = 3  # Number of glow layers
GLOW_ALPHA_MULTIPLIER = 0.15  # Alpha per layer

# --- Colors (Dark Mode) ---
DARK_BG_COLOR = "#0D1117"  # Main background
DARK_CARD_BG = "#161B22"  # Card background
DARK_TEXT_COLOR = "#E6EDF3"  # Primary text
DARK_TEXT_SECONDARY = "#8B949E"  # Secondary text (subtitle, footer)
DARK_GRID_COLOR = "#30363D"  # Card border color

# --- Colors (Light Mode) ---
LIGHT_BG_COLOR = "#FFFFFF"
LIGHT_CARD_BG = "#F6F8FA"
LIGHT_TEXT_COLOR = "#1F2328"
LIGHT_TEXT_SECONDARY = "#656D76"
LIGHT_GRID_COLOR = "#D0D7DE"

# --- Bar Colors (primary, secondary for each system) ---
# Softer, more muted palette
BAR_COLORS = {
    "timeseries-table": ("#E07A5F", "#E8998D"),  # Terracotta (warm but muted)
    "ClickHouse": ("#F2CC8F", "#F5DDB5"),  # Warm sand
    "Delta+Spark": ("#81B29A", "#A3C4B1"),  # Sage green
    "PostgreSQL": ("#5B8FB9", "#89B4D4"),  # Soft blue
    "TimescaleDB": ("#9A8C98", "#B5AAB8"),  # Muted purple/mauve
}

# --- System Data ---
SYSTEMS = ["timeseries-table", "ClickHouse", "Delta+Spark", "PostgreSQL", "TimescaleDB"]
DISPLAY_NAMES = [
    "timeseries-table",
    "ClickHouse",
    "Delta + Spark",
    "PostgreSQL",
    "TimescaleDB",
]
HERO_SYSTEM = "timeseries-table"  # Which system to highlight

# --- Benchmark Data (in milliseconds) ---
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
        "Delta+Spark": 964,
        "PostgreSQL": 43556,
        "TimescaleDB": 43923,
    },
}

# =============================================================================
# END CONFIGURATION
# =============================================================================

# Font setup
plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Arial", "Helvetica", "sans-serif"]


def format_time(ms: float) -> str:
    """Format milliseconds to human readable string."""
    if ms >= 1000:
        return f"{ms / 1000:.1f}s"
    return f"{int(ms)}ms"


def draw_rounded_bar(ax, x, y, width, height, color, alpha=1.0, glow=False):
    """Draw a rounded rectangle bar with optional glow effect."""
    if width <= 0:
        return

    rounding = height / BAR_ROUNDING_FACTOR

    # Glow effect for hero bar
    if glow and GLOW_ENABLED:
        for i in range(GLOW_LAYERS, 0, -1):
            glow_bar = FancyBboxPatch(
                (x, y - height / 2),
                width,
                height,
                boxstyle=f"round,pad=0,rounding_size={rounding}",
                facecolor=color,
                edgecolor="none",
                alpha=GLOW_ALPHA_MULTIPLIER * (GLOW_LAYERS + 1 - i),
                transform=ax.transData,
                zorder=1,
            )
            ax.add_patch(glow_bar)

    # Main bar
    bar = FancyBboxPatch(
        (x, y - height / 2),
        width,
        height,
        boxstyle=f"round,pad=0,rounding_size={rounding}",
        facecolor=color,
        edgecolor="none",
        alpha=alpha,
        transform=ax.transData,
        zorder=2,
    )
    ax.add_patch(bar)
    return bar


def create_modern_chart(dark_mode=True):
    """Create a modern benchmark chart."""

    # Colors based on mode
    if dark_mode:
        bg_color = DARK_BG_COLOR
        card_bg = DARK_CARD_BG
        text_color = DARK_TEXT_COLOR
        text_secondary = DARK_TEXT_SECONDARY
        grid_color = DARK_GRID_COLOR
    else:
        bg_color = LIGHT_BG_COLOR
        card_bg = LIGHT_CARD_BG
        text_color = LIGHT_TEXT_COLOR
        text_secondary = LIGHT_TEXT_SECONDARY
        grid_color = LIGHT_GRID_COLOR

    fig = plt.figure(figsize=(FIG_WIDTH, FIG_HEIGHT), facecolor=bg_color)

    # Title area
    fig.text(
        0.5,
        TITLE_Y,
        TITLE_TEXT,
        fontsize=TITLE_FONTSIZE,
        fontweight="bold",
        color=text_color,
        ha="center",
        va="top",
    )
    fig.text(
        0.5,
        SUBTITLE_Y,
        SUBTITLE_TEXT,
        fontsize=SUBTITLE_FONTSIZE,
        color=text_secondary,
        ha="center",
        va="top",
    )

    operations = list(DATA.keys())

    # Create 3 card-style subplots
    for idx, op in enumerate(operations):
        # Position each "card"
        card_left = CARD_LEFT_START + idx * CARD_SPACING

        ax = fig.add_axes((card_left, CARD_BOTTOM, CARD_WIDTH, CARD_HEIGHT))
        ax.set_facecolor(card_bg)

        # Remove spines
        for spine in ax.spines.values():
            spine.set_visible(False)

        # Card background with subtle border
        card_rect = FancyBboxPatch(
            (0, 0),
            1,
            1,
            boxstyle=f"round,pad=0.02,rounding_size={CARD_ROUNDING}",
            facecolor=card_bg,
            edgecolor=grid_color,
            linewidth=CARD_BORDER_WIDTH,
            transform=ax.transAxes,
            zorder=0,
        )
        ax.add_patch(card_rect)

        values = [DATA[op][sys] for sys in SYSTEMS]
        max_val = max(values)

        # Draw bars
        for i, (sys, val, name) in enumerate(zip(SYSTEMS, values, DISPLAY_NAMES)):
            y = BAR_START_Y - i * BAR_SPACING
            bar_width = (val / max_val) * BAR_MAX_WIDTH

            color = BAR_COLORS[sys][0]
            is_hero = sys == HERO_SYSTEM

            draw_rounded_bar(
                ax,
                BAR_LEFT_MARGIN,
                y,
                bar_width,
                BAR_HEIGHT,
                color,
                alpha=BAR_HERO_ALPHA if is_hero else BAR_OTHER_ALPHA,
                glow=is_hero,
            )

            # System name (left aligned, above bar)
            ax.text(
                BAR_LEFT_MARGIN,
                y + BAR_HEIGHT / 2 + LABEL_OFFSET_Y,
                name,
                fontsize=LABEL_FONTSIZE,
                color=text_color,
                ha="left",
                va="bottom",
                fontweight="bold" if is_hero else "normal",
                transform=ax.transAxes,
            )

            # Time value (at end of bar or outside)
            time_str = format_time(val)
            if bar_width > TIME_INSIDE_THRESHOLD:
                ax.text(
                    BAR_LEFT_MARGIN + bar_width - TIME_INSIDE_OFFSET,
                    y,
                    time_str,
                    fontsize=TIME_FONTSIZE,
                    color="white",
                    ha="right",
                    va="center",
                    fontweight="bold",
                    transform=ax.transAxes,
                )
            else:
                ax.text(
                    BAR_LEFT_MARGIN + bar_width + TIME_OUTSIDE_OFFSET,
                    y,
                    time_str,
                    fontsize=TIME_FONTSIZE,
                    color=text_color,
                    ha="left",
                    va="center",
                    fontweight="bold",
                    transform=ax.transAxes,
                )

            # Speedup badge for hero
            if is_hero and i == 0:
                speedup = values[1] / val  # vs second place
                badge_text = f"{speedup:.0f}× faster"
                ax.text(
                    BADGE_X,
                    y,
                    badge_text,
                    fontsize=BADGE_FONTSIZE,
                    color=BAR_COLORS[HERO_SYSTEM][0],
                    ha="right",
                    va="center",
                    fontweight="bold",
                    transform=ax.transAxes,
                    bbox=dict(
                        boxstyle=f"round,pad={BADGE_PADDING}",
                        facecolor=BAR_COLORS[HERO_SYSTEM][0],
                        alpha=BADGE_ALPHA,
                        edgecolor="none",
                    ),
                )

        # Operation title at bottom
        ax.text(
            0.5,
            OP_TITLE_Y,
            op,
            fontsize=OP_TITLE_FONTSIZE,
            fontweight="bold",
            color=text_color,
            ha="center",
            va="bottom",
            transform=ax.transAxes,
        )

        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_xticks([])
        ax.set_yticks([])

    # Footer
    fig.text(
        0.5,
        FOOTER_Y,
        FOOTER_TEXT,
        fontsize=FOOTER_FONTSIZE,
        color=text_secondary,
        ha="center",
        style="italic",
    )

    return fig


def main():
    output_dir = Path(__file__).parent.parent.parent / "docs" / "assets"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Dark mode version
    fig_dark = create_modern_chart(dark_mode=True)
    output_dark = output_dir / "benchmark-chart.png"
    fig_dark.savefig(
        output_dark,
        dpi=FIG_DPI,
        bbox_inches="tight",
        facecolor=DARK_BG_COLOR,
        edgecolor="none",
        pad_inches=0.2,
    )
    plt.close(fig_dark)
    print(f"✓ Saved dark mode: {output_dark}")

    # Light mode version
    fig_light = create_modern_chart(dark_mode=False)
    output_light = output_dir / "benchmark-chart-light.png"
    fig_light.savefig(
        output_light,
        dpi=FIG_DPI,
        bbox_inches="tight",
        facecolor="white",
        edgecolor="none",
        pad_inches=0.2,
    )
    plt.close(fig_light)
    print(f"✓ Saved light mode: {output_light}")


if __name__ == "__main__":
    main()
