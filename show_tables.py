"""
CogniPulse - Postgres Table Viewer
Run: python show_tables.py
Requires: pip install psycopg2-binary
"""

import sys
import textwrap
import psycopg2
from datetime import datetime

# Force UTF-8 output on Windows
sys.stdout.reconfigure(encoding="utf-8")

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="cogni",
    user="cogni",
    password="cogni"
)

cur = conn.cursor()

WRAP_WIDTH = 50


def ts(epoch):
    try:
        return datetime.fromtimestamp(int(epoch)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(epoch)


def truncate(text, max_len=60):
    text = str(text) if text is not None else "-"
    return text if len(text) <= max_len else text[:max_len - 3] + "..."


def print_table(title, headers, rows, formatters=None, wrap_cols=None):
    if not rows:
        print(f"\n{'='*60}\n  {title}\n{'='*60}")
        print("  (no data yet)")
        return

    wrap_cols = wrap_cols or set()

    formatted = []
    for row in rows:
        row = list(row)
        if formatters:
            for i, fn in formatters.items():
                if fn and i < len(row):
                    row[i] = fn(row[i])
        formatted.append([str(c) if c is not None else "-" for c in row])

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

    col_widths = [len(h) for h in headers]
    for row in wrapped:
        for i, lines in enumerate(row):
            for line in lines:
                col_widths[i] = max(col_widths[i], len(line))

    sep        = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
    header_row = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"

    print(f"\n{'='*len(sep)}\n  {title}\n{'='*len(sep)}")
    print(sep)
    print(header_row)
    print(sep)

    for row in wrapped:
        n_lines = max(len(lines) for lines in row)
        for line_idx in range(n_lines):
            parts = []
            for i, lines in enumerate(row):
                cell_line = lines[line_idx] if line_idx < len(lines) else ""
                parts.append(cell_line.ljust(col_widths[i]))
            print("| " + " | ".join(parts) + " |")
        print(sep)

    print(f"  {len(rows)} row(s)\n")


# --- 1. Latest Device State ---------------------------------------------------
cur.execute("SELECT device_id, last_seen, last_temperature, last_vibration, status FROM latest_state ORDER BY device_id")
print_table(
    title="LATEST STATE (current status per device)",
    headers=["Device ID", "Last Seen", "Temp (C)", "Vibration", "Status"],
    rows=cur.fetchall(),
    formatters={1: ts, 2: lambda v: f"{v:.2f}", 3: lambda v: f"{v:.4f}"}
)

# --- 2. Recent Telemetry Events -----------------------------------------------
cur.execute("SELECT device_id, timestamp, temperature, vibration FROM telemetry_events ORDER BY id DESC LIMIT 20")
print_table(
    title="TELEMETRY EVENTS (last 20 raw sensor readings)",
    headers=["Device ID", "Timestamp", "Temp (C)", "Vibration"],
    rows=cur.fetchall(),
    formatters={1: ts, 2: lambda v: f"{v:.2f}", 3: lambda v: f"{v:.4f}"}
)

# --- 3. Alert Events ----------------------------------------------------------
cur.execute("SELECT device_id, timestamp, alert_type, severity, reason FROM alert_events ORDER BY id DESC LIMIT 20")
print_table(
    title="ALERT EVENTS (last 20 anomalies detected by analyzer)",
    headers=["Device ID", "Timestamp", "Alert Type", "Severity", "Reason"],
    rows=cur.fetchall(),
    formatters={1: ts}
)

# --- 4. Action Events ---------------------------------------------------------
cur.execute("SELECT device_id, timestamp, action_type, confidence, decision_reason FROM action_events ORDER BY id DESC LIMIT 20")
print_table(
    title="ACTION EVENTS (last 20 LLM decisions)",
    headers=["Device ID", "Timestamp", "Action", "Conf", "Reasoning (truncated)"],
    rows=cur.fetchall(),
    formatters={
        1: ts,
        3: lambda v: f"{float(v):.2f}",
        4: lambda v: truncate(v, 60)
    }
)

# --- 5. Action Lineage --------------------------------------------------------
cur.execute("""
    SELECT
        act.id,
        act.device_id,
        act.action_type,
        act.timestamp,
        act.confidence,
        al.id,
        al.alert_type,
        al.severity,
        al.timestamp,
        al.reason,
        te.id,
        te.temperature,
        te.vibration,
        te.timestamp
    FROM action_events act
    LEFT JOIN alert_events al ON act.alert_event_id = al.id
    LEFT JOIN telemetry_events te ON al.telemetry_event_id = te.id
    ORDER BY act.id DESC LIMIT 20
""")
rows = cur.fetchall()

print(f"\n{'='*70}")
print("  ACTION LINEAGE  (action -> alert -> telemetry event)")
print(f"{'='*70}")

if not rows:
    print("  (no data yet)")
else:
    for r in rows:
        act_id, device, action, act_ts, conf, al_id, alert_type, severity, al_ts, reason, te_id, temp, vib, te_ts = r

        conf_str   = f"{float(conf):.2f}" if conf is not None else "-"
        temp_str   = f"{float(temp):.2f} C" if temp is not None else "-"
        vib_str    = f"{float(vib):.4f}" if vib is not None else "-"
        reason_str = truncate(reason, 70) if reason else "-"

        print(f"\n  ACTION  #{act_id:<4}  {device:<12}  {action:<10}  {ts(act_ts)}  conf: {conf_str}")
        print(f"  {'-'*66}")
        if al_id:
            print(f"  +-ALERT  #{al_id:<4}  {alert_type:<10}  {severity:<10}  {ts(al_ts)}")
            print(f"  |        reason: {reason_str}")
        else:
            print("  +-ALERT  (none)")
        if te_id:
            print(f"  +-EVENT  #{te_id:<4}  temp: {temp_str:<10}  vib: {vib_str:<10}  {ts(te_ts)}")
        else:
            print("  +-EVENT  (none)")

    print(f"\n  {len(rows)} record(s)\n")

cur.close()
conn.close()
