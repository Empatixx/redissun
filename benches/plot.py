#!/usr/bin/env python3
"""Turns benches/results/*.json into two SVG charts (standard library only).

Every JSON file is one run. When a library has several runs, the chart shows the median of them.

    python3 benches/plot.py            # writes website/public/bench/latency.svg and throughput.svg
"""
import glob
import json
import os
import statistics
import sys
from xml.sax.saxutils import escape

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, "..", "website", "public", "bench")

# Fixed order of the categorical slots; a library keeps its colour in every chart.
LIBRARIES = [
    ("redissun", "redissun", "s1"),
    ("redis-rs", "redis-rs", "s2"),
    ("fred", "fred", "s3"),
    ("redisson", "Redisson (Java)", "s4"),
]
OPERATIONS = [
    ("bucket_set", "Bucket set"),
    ("bucket_get", "Bucket get"),
    ("map_insert", "HashMap insert"),
    ("map_get", "HashMap get"),
    ("lock_unlock", "Lock and unlock"),
    ("batch_100", "Batch of 100 sets"),
]

STYLE = """
  svg { color-scheme: light dark; }
  .surface { fill: #fcfcfb; }
  .ink { fill: #0b0b0b; }
  .ink2 { fill: #52514e; }
  .grid { stroke: #e3e2de; stroke-width: 1; }
  .s1 { fill: #2a78d6; } .s2 { fill: #eb6834; } .s3 { fill: #1baf7a; } .s4 { fill: #eda100; }
  text { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; }
  @media (prefers-color-scheme: dark) {
    .surface { fill: #1a1a19; }
    .ink { fill: #f0efec; }
    .ink2 { fill: #c3c2b7; }
    .grid { stroke: #343432; }
    .s1 { fill: #3987e5; } .s2 { fill: #d95926; } .s3 { fill: #199e70; } .s4 { fill: #c98500; }
  }
"""


def load():
    runs = {}
    for path in sorted(glob.glob(os.path.join(ROOT, "results", "*.json"))):
        with open(path) as handle:
            document = json.load(handle)
        for row in document["results"]:
            runs.setdefault((row["library"], row["operation"]), []).append(row)
    table = {}
    for key, rows in runs.items():
        def middle(field):
            values = [row[field] for row in rows if row.get(field) is not None]
            return statistics.median(values) if values else None

        table[key] = {
            "p50_us": middle("p50_us"),
            "p99_us": middle("p99_us"),
            "ops_per_second": middle("ops_per_second"),
            "runs": len(rows),
        }
    return table


def nice_ceiling(value):
    for step in (1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        scale = 10 ** (len(str(int(value))) - 1)
        if step * scale >= value:
            return step * scale
    return value


def chart(table, metric, title, subtitle, fmt, tick_fmt, operations, filename):
    libraries = [lib for lib in LIBRARIES if any((lib[0], op[0]) in table for op in operations)]
    left, right, top = 150, 78, 74
    bar, gap, group_gap = 13, 2, 18
    group_height = len(libraries) * bar + (len(libraries) - 1) * gap
    height = top + len(operations) * (group_height + group_gap) + 34
    width = 760
    plot = width - left - right

    values = [
        table[(lib[0], op[0])][metric]
        for lib in libraries
        for op in operations
        if (lib[0], op[0]) in table and table[(lib[0], op[0])][metric] is not None
    ]
    top_value = nice_ceiling(max(values))
    ticks = [top_value * i / 4 for i in range(5)]

    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="{width}" '
        f'height="{height}" role="img" aria-label="{escape(title)}">',
        f"<title>{escape(title)}</title>",
        f"<style>{STYLE}</style>",
        f'<rect class="surface" width="{width}" height="{height}" rx="8"/>',
        f'<text class="ink" x="20" y="28" font-size="15" font-weight="600">{escape(title)}</text>',
        f'<text class="ink2" x="20" y="46" font-size="12">{escape(subtitle)}</text>',
    ]

    legend_x = left
    for _, label, css in libraries:
        out.append(f'<rect class="{css}" x="{legend_x}" y="57" width="10" height="10" rx="2"/>')
        out.append(f'<text class="ink2" x="{legend_x + 15}" y="66" font-size="11">{escape(label)}</text>')
        legend_x += 15 + 6.6 * len(label) + 18

    axis_y = top + len(operations) * (group_height + group_gap) - group_gap + 8
    for tick in ticks:
        x = left + plot * tick / top_value
        out.append(f'<line class="grid" x1="{x:.1f}" y1="{top - 4}" x2="{x:.1f}" y2="{axis_y}"/>')
        out.append(
            f'<text class="ink2" x="{x:.1f}" y="{axis_y + 16}" font-size="11" text-anchor="middle">'
            f"{escape(tick_fmt(tick))}</text>"
        )

    for row, (op_key, op_label) in enumerate(operations):
        y0 = top + row * (group_height + group_gap)
        out.append(
            f'<text class="ink" x="{left - 12}" y="{y0 + group_height / 2 + 4:.1f}" font-size="12" '
            f'text-anchor="end">{escape(op_label)}</text>'
        )
        for index, (lib_key, label, css) in enumerate(libraries):
            entry = table.get((lib_key, op_key))
            if entry is None or entry[metric] is None:
                continue
            value = entry[metric]
            y = y0 + index * (bar + gap)
            length = max(2.0, plot * value / top_value)
            radius = min(4, length / 2)
            # a bar with a 4px rounded data end and a square end on the baseline
            path = (
                f"M{left},{y} h{length - radius:.1f} a{radius},{radius} 0 0 1 {radius},{radius} "
                f"v{bar - 2 * radius} a{radius},{radius} 0 0 1 -{radius},{radius} h-{length - radius:.1f} z"
            )
            tip = f"{label}, {op_label}: {fmt(value)}"
            out.append(f'<path class="{css}" d="{path}"><title>{escape(tip)}</title></path>')
            out.append(
                f'<text class="ink2" x="{left + length + 6:.1f}" y="{y + bar - 3}" font-size="10.5">'
                f"{escape(fmt(value))}</text>"
            )
    out.append("</svg>")

    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, filename)
    with open(path, "w") as handle:
        handle.write("\n".join(out) + "\n")
    print("wrote", os.path.relpath(path))


def micro(value):
    return f"{value:,.0f} µs"


def per_second(value):
    return f"{value / 1000:,.0f}k/s"


def main():
    table = load()
    if not table:
        sys.exit("no results in benches/results/")
    chart(
        table,
        "p50_us",
        "Latency of one operation",
        "Median time of one call from one task, in microseconds. Lower is better.",
        micro,
        lambda tick: f"{tick:,.0f}",
        OPERATIONS,
        "latency.svg",
    )
    chart(
        table,
        "ops_per_second",
        "Throughput with 64 tasks",
        "Operations per second, 64 tasks on different keys. Higher is better.",
        per_second,
        lambda tick: f"{tick / 1000:,.0f}k",
        [op for op in OPERATIONS if op[0] != "batch_100"],
        "throughput.svg",
    )
    # the numbers behind the charts, for the docs page
    rows = []
    for lib_key, label, _ in LIBRARIES:
        for op_key, op_label in OPERATIONS:
            entry = table.get((lib_key, op_key))
            if entry:
                rows.append({"library": label, "operation": op_label, **entry})
    with open(os.path.join(OUT, "summary.json"), "w") as handle:
        json.dump(rows, handle, indent=2)


if __name__ == "__main__":
    main()
