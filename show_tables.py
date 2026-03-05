"""
CogniPulse — Postgres Table Viewer
Run: python show_tables.py
Requires: pip install psycopg2-binary
"""

import textwrap
import psycopg2
from datetime import datetime

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="cogni",
    user="cogni",
    password="cogni"
)

cur = conn.cursor()

# Max width for any single column before it wraps
WRAP_WIDTH = 55


def ts(epoch):
    try:
        return datetime.fromtimestamp(int(epoch)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(epoch)


def print_table(title, headers, rows, formatters=None, wrap_cols=None):
    """Print a table with word-wrapped columns."""
    if not rows:
        print(f"\n{'='*60}")
        print(f"  {title}")
        print(f"{'='*60}")
        print("  (no data yet)")
        return

    wrap_cols = wrap_cols or set()

    # Apply formatters and convert to strings
    formatted = []
    for row in rows:
        row = list(row)
        if formatters:
            for i, fn in formatters.items():
                if fn and i < len(row):
                    row[i] = fn(row[i])
        formatted.append([str(c) if c is not None else "—" for c in row])

    # Wrap long columns into lists of lines per cell
    wrapped = []
    for row in formatted:
        wrapped_row = []
        for i, cell in enumerate(row):
            cell = cell.replace("\n", " ").strip()
            if i in wrap_cols and len(cell) > WRAP_WIDTH:
                lines = textwrap.wrap(cell, WRAP_WIDTH)
            else:
                lines = [cell]
            wrapped_row.append(lines)
        wrapped.append(wrapped_row)

    # Column widths: max of header or longest line in that column
    col_widths = [len(h) for h in headers]
    for row in wrapped:
        for i, lines in enumerate(row):
            for line in lines:
                col_widths[i] = max(col_widths[i], len(line))

    sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
    header_row = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"

    print(f"\n{'='*len(sep)}")
    print(f"  {title}")
    print(f"{'='*len(sep)}")
    print(sep)
    print(header_row)
    print(sep)

    for row in wrapped:
        # Find how many lines this row needs
        n_lines = max(len(lines) for lines in row)
        for line_idx in range(n_lines):
            parts = []
            for i, lines in enumerate(row):
                cell_line = lines[line_idx] if line_idx < len(lines) else ""
                parts.append(cell_line.ljust(col_widths[i]))
            print("| " + " | ".join(parts) + " |")
        print(sep)

    print(f"  {len(rows)} row(s)\n")


# ─── 1. Latest Device State ──────────────────────────────────────────────────
cur.execute("SELECT device_id, last_seen, last_temperature, last_vibration, status FROM latest_state ORDER BY device_id")
print_table(
    title="LATEST STATE (current status per device)",
    headers=["Device ID", "Last Seen", "Temperature (°C)", "Vibration", "Status"],
    rows=cur.fetchall(),
    formatters={1: ts, 2: lambda v: f"{v:.2f}", 3: lambda v: f"{v:.4f}"}
)

# ─── 2. Recent Telemetry Events ──────────────────────────────────────────────
cur.execute("SELECT device_id, timestamp, temperature, vibration FROM telemetry_events ORDER BY id DESC LIMIT 20")
print_table(
    title="TELEMETRY EVENTS (last 20 raw sensor readings)",
    headers=["Device ID", "Timestamp", "Temperature (°C)", "Vibration"],
    rows=cur.fetchall(),
    formatters={1: ts, 2: lambda v: f"{v:.2f}", 3: lambda v: f"{v:.4f}"}
)

# ─── 3. Alert Events ─────────────────────────────────────────────────────────
cur.execute("SELECT device_id, timestamp, alert_type, severity, reason FROM alert_events ORDER BY id DESC LIMIT 20")
print_table(
    title="ALERT EVENTS (last 20 anomalies detected by analyzer)",
    headers=["Device ID", "Timestamp", "Alert Type", "Severity", "Reason"],
    rows=cur.fetchall(),
    formatters={1: ts}
)

# ─── 4. Action Events ────────────────────────────────────────────────────────
cur.execute("SELECT device_id, timestamp, action_type, confidence, decision_reason FROM action_events ORDER BY id DESC LIMIT 20")
print_table(
    title="ACTION EVENTS (last 20 LLM decisions)",
    headers=["Device ID", "Timestamp", "Action", "Confidence", "Reasoning"],
    rows=cur.fetchall(),
    formatters={1: ts, 3: lambda v: f"{float(v):.2f}"},
    wrap_cols={4}
)

cur.close()
conn.close()
