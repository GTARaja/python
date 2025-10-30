#!/usr/bin/env python3
"""
Fast Oracle RMS Query Runner (Full featured)

- Uses GLOBAL TEMPORARY TABLES to materialize intermediate results
- Applies parallel/append hints for fast bulk population
- Creates indexes on temp tables for fast joins
- Gathers stats (if permitted)
- Captures runtime execution plan (DBMS_XPLAN.DISPLAY_CURSOR with ALLSTATS LAST)
- Logs detailed timestamps, durations, rowcounts to a timestamped logfile
- Writes a CSV summary for comparison
- Drops temp tables and cleanup at the end

Requirements:
- python-oracledb (pip install oracledb)
- Oracle client environment / network access to DB
"""

import oracledb
import time
import os
import csv
from datetime import datetime

# -------------------------
# CONFIGURATION
# -------------------------
DB_CONFIG = {
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    # Use host:port/service format for easy connect
    "dsn": "YOUR_HOST:1521/YOUR_SERVICE"
}

# Tune these values according to your DB server CPU / memory
PARALLEL_DEGREE = 8
PGA_TARGET = "1G"

# Working files / directories
LOG_DIR = "oracle_query_logs"
CSV_SUMMARY = os.path.join(LOG_DIR, "summary.csv")
os.makedirs(LOG_DIR, exist_ok=True)

RUN_TS = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = os.path.join(LOG_DIR, f"oracle_query_{RUN_TS}.log")

# Query-specific constants (change if semantics differ)
EXPECTED_VALID_LOCS = 240
EXPECTED_IM_LIMIT = 2000
TARGET_WH_LOC = 486
TARGET_WH_STATUS = 'A'

# -------------------------
# SQL FRAGMENTS
# -------------------------
CREATE_TEMP_S2 = """
CREATE GLOBAL TEMPORARY TABLE TEMP_S2 (
    loc NUMBER
) ON COMMIT PRESERVE ROWS
"""

CREATE_TEMP_IM = """
CREATE GLOBAL TEMPORARY TABLE TEMP_IM (
    al NUMBER,
    bl NUMBER,
    loc_count NUMBER
) ON COMMIT PRESERVE ROWS
"""

CREATE_TEMP_WH = """
CREATE GLOBAL TEMPORARY TABLE TEMP_WH (
    a NUMBER
) ON COMMIT PRESERVE ROWS
"""

POPULATE_S2 = f"""
INSERT /*+ PARALLEL( {PARALLEL_DEGREE} ) APPEND */ INTO TEMP_S2
SELECT loc
FROM rms.item_loc
WHERE status = 'A'
GROUP BY loc
HAVING COUNT(item) > 2000
FETCH FIRST {EXPECTED_VALID_LOCS} ROWS ONLY
"""

POPULATE_IM = f"""
INSERT /*+ PARALLEL( {PARALLEL_DEGREE} ) APPEND */ INTO TEMP_IM
SELECT i3.item AS al,
       i3.loc  AS bl,
       COUNT(i3.loc) AS loc_count
FROM rms.item_loc i3
WHERE i3.loc_type = 'S'
  AND i3.status = 'A'
  AND i3.loc IN (SELECT loc FROM TEMP_S2)
GROUP BY i3.item, i3.loc
HAVING COUNT(DISTINCT i3.loc) = {EXPECTED_VALID_LOCS}
FETCH FIRST {EXPECTED_IM_LIMIT} ROWS ONLY
"""

POPULATE_WH = f"""
INSERT /*+ PARALLEL( {PARALLEL_DEGREE} ) APPEND */ INTO TEMP_WH
SELECT item AS a
FROM rms.item_loc
WHERE loc = :target_loc
  AND status = :target_status
"""

FINAL_SELECT = """
SELECT im.al, im.bl
FROM TEMP_IM im
JOIN TEMP_WH wh ON wh.a = im.al
JOIN TEMP_S2 s2 ON im.bl = s2.loc
"""

CREATE_IDX_TEMP_IM_AL = "CREATE INDEX IDX_TEMP_IM_AL ON TEMP_IM(al)"
CREATE_IDX_TEMP_IM_BL = "CREATE INDEX IDX_TEMP_IM_BL ON TEMP_IM(bl)"
CREATE_IDX_TEMP_S2_LOC = "CREATE INDEX IDX_TEMP_S2_LOC ON TEMP_S2(loc)"
CREATE_IDX_TEMP_WH_A = "CREATE INDEX IDX_TEMP_WH_A ON TEMP_WH(a)"

GATHER_STATS_ON_TEMP = """
BEGIN
  DBMS_STATS.GATHER_TABLE_STATS(USER, :tbl, cascade => TRUE, estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE);
END;
"""

EXPLAIN_PLAN_FOR = "EXPLAIN PLAN FOR "  # we'll append the final select to this if needed

FETCH_RUNTIME_PLAN = "SELECT PLAN_TABLE_OUTPUT FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'ALLSTATS LAST'))"

# -------------------------
# UTILITIES
# -------------------------
def write_log(line: str):
    ts = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    entry = f"{ts} {line}"
    print(entry)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(entry + "\n")

def append_summary_row(summary_row: dict):
    header = [
        "run_ts", "phase", "duration_sec", "rows", "notes"
    ]
    write_header = not os.path.exists(CSV_SUMMARY)
    with open(CSV_SUMMARY, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        if write_header:
            writer.writeheader()
        writer.writerow(summary_row)

# -------------------------
# MAIN FLOW
# -------------------------
def run():
    conn = None
    try:
        write_log("Starting fast RMS query run (temp-table based).")
        write_log(f"Connecting to Oracle using DSN: {DB_CONFIG['dsn']} ...")

        conn = oracledb.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Session tuning
        write_log("Applying session-level optimizer / parallel settings...")
        try:
            cur.execute("ALTER SESSION ENABLE PARALLEL QUERY")
            cur.execute(f"ALTER SESSION FORCE PARALLEL QUERY PARALLEL {PARALLEL_DEGREE}")
            cur.execute("ALTER SESSION SET optimizer_parallel_execution_enabled = TRUE")
            cur.execute("ALTER SESSION SET workarea_size_policy = AUTO")
            cur.execute(f"ALTER SESSION SET pga_aggregate_target = {PGA_TARGET}")
            cur.execute("ALTER SESSION SET statistics_level = ALL")
            cur.execute("ALTER SESSION SET timed_statistics = TRUE")
        except Exception as e:
            write_log(f"Warning: Could not apply one or more session settings (permissions?). Error: {e}")

        # Clean previous temp tables if any (best-effort)
        temp_tables = ["TEMP_IM", "TEMP_S2", "TEMP_WH"]
        for t in temp_tables:
            try:
                cur.execute(f"DROP TABLE {t} PURGE")
                write_log(f"Dropped pre-existing table {t}")
            except Exception:
                pass

        # Create temp tables
        write_log("Creating GLOBAL TEMPORARY TABLES...")
        cur.execute(CREATE_TEMP_S2); cur.execute(CREATE_TEMP_IM); cur.execute(CREATE_TEMP_WH)
        conn.commit()
        write_log("Created temp tables.")

        # Populate TEMP_S2
        t0 = time.perf_counter()
        write_log("Populating TEMP_S2 (locs with > 2000 items, top 240)...")
        cur.execute(POPULATE_S2)
        conn.commit()
        t1 = time.perf_counter()
        # rows count
        cur.execute("SELECT COUNT(*) FROM TEMP_S2")
        s2_count = cur.fetchone()[0]
        write_log(f"TEMP_S2 populated: {s2_count} rows. Duration: {t1 - t0:.2f}s")
        append_summary_row({
            "run_ts": RUN_TS, "phase": "populate_s2", "duration_sec": round(t1-t0,2), "rows": s2_count,
            "notes": f"PARALLEL={PARALLEL_DEGREE}"
        })

        # Populate TEMP_IM
        t0 = time.perf_counter()
        write_log("Populating TEMP_IM (items at 'S' locs across all selected locs)...")
        cur.execute(POPULATE_IM)
        conn.commit()
        t1 = time.perf_counter()
        cur.execute("SELECT COUNT(*) FROM TEMP_IM")
        im_count = cur.fetchone()[0]
        write_log(f"TEMP_IM populated: {im_count} rows. Duration: {t1 - t0:.2f}s")
        append_summary_row({
            "run_ts": RUN_TS, "phase": "populate_im", "duration_sec": round(t1-t0,2), "rows": im_count,
            "notes": f"EXPECTED_VALID_LOCS={EXPECTED_VALID_LOCS}"
        })

        # Create indexes on temp tables to speed final join (only if rows > 0)
        if im_count > 0 and s2_count > 0:
            write_log("Creating indexes on TEMP_IM, TEMP_S2, TEMP_WH for faster joins...")
            try:
                cur.execute(CREATE_IDX_TEMP_IM_AL)
                cur.execute(CREATE_IDX_TEMP_IM_BL)
                cur.execute(CREATE_IDX_TEMP_S2_LOC)
                conn.commit()
                write_log("Created temp-table indexes.")
            except Exception as e:
                write_log(f"Warning: Could not create one or more indexes: {e}")

            # Gather stats on temporary tables (optional - requires privileges)
            try:
                write_log("Gathering stats on TEMP_S2 and TEMP_IM (if allowed)...")
                cur.execute(GATHER_STATS_ON_TEMP, {"tbl": "TEMP_S2"})
                cur.execute(GATHER_STATS_ON_TEMP, {"tbl": "TEMP_IM"})
                conn.commit()
                write_log("Stats gather completed (if permitted).")
            except Exception as e:
                write_log(f"Warning: Stats gather skipped or failed: {e}")

        # Populate TEMP_WH (items in loc 486 with status 'A')
        t0 = time.perf_counter()
        write_log(f"Populating TEMP_WH for loc={TARGET_WH_LOC}, status='{TARGET_WH_STATUS}'...")
        cur.execute(POPULATE_WH, {"target_loc": TARGET_WH_LOC, "target_status": TARGET_WH_STATUS})
        conn.commit()
        t1 = time.perf_counter()
        cur.execute("SELECT COUNT(*) FROM TEMP_WH")
        wh_count = cur.fetchone()[0]
        write_log(f"TEMP_WH populated: {wh_count} rows. Duration: {t1 - t0:.2f}s")
        append_summary_row({
            "run_ts": RUN_TS, "phase": "populate_wh", "duration_sec": round(t1-t0,2), "rows": wh_count,
            "notes": f"loc={TARGET_WH_LOC}"
        })

        # Optional index on TEMP_WH
        if wh_count > 0:
            try:
                cur.execute(CREATE_IDX_TEMP_WH_A)
                conn.commit()
                write_log("Index created on TEMP_WH(a).")
            except Exception as e:
                write_log(f"Warning: Could not create index on TEMP_WH: {e}")

        # Final select (measure it)
        t0 = time.perf_counter()
        write_log("Executing final join SELECT (TEMP_IM JOIN TEMP_WH JOIN TEMP_S2)...")
        cur.execute(FINAL_SELECT)
        rows = cur.fetchall()
        t1 = time.perf_counter()
        final_rows = len(rows)
        final_duration = t1 - t0
        write_log(f"Final SELECT completed. Rows returned: {final_rows}. Duration: {final_duration:.2f}s")
        append_summary_row({
            "run_ts": RUN_TS, "phase": "final_select", "duration_sec": round(final_duration,2), "rows": final_rows,
            "notes": "final join"
        })

        # Capture runtime execution plan and detailed statistics
        write_log("Capturing runtime execution plan (DBMS_XPLAN.DISPLAY_CURSOR with ALLSTATS LAST)...")
        try:
            cur.execute(FETCH_RUNTIME_PLAN)
            plan_lines = [r[0] for r in cur.fetchall()]
            plan_text = "\n".join(plan_lines)
            write_log("---- START EXECUTION PLAN ----")
            for ln in plan_text.splitlines():
                write_log(ln)
            write_log("---- END EXECUTION PLAN ----")
            # also write plan to a separate file for convenience
            plan_file = os.path.join(LOG_DIR, f"plan_{RUN_TS}.txt")
            with open(plan_file, "w", encoding="utf-8") as pf:
                pf.write(plan_text)
            write_log(f"Execution plan saved to {plan_file}")
        except Exception as e:
            write_log(f"Warning: Could not fetch runtime plan: {e}")

        # Capture elapsed for full run
        write_log("Cleaning up: dropping temp tables and indexes.")
        try:
            cur.execute("DROP TABLE TEMP_IM PURGE")
            cur.execute("DROP TABLE TEMP_S2 PURGE")
            cur.execute("DROP TABLE TEMP_WH PURGE")
            conn.commit()
            write_log("Temporary tables dropped.")
        except Exception as e:
            write_log(f"Warning: Could not drop some temp tables: {e}")

        total_runtime = sum([
            # we logged each phase individually into CSV; compute approximate total by summing durations in that CSV if desired
            # simpler: mark end time now
        ])
        write_log(f"Run completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as general_e:
        write_log(f"ERROR during run: {general_e}")
    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
                write_log("Database connection closed.")
        except Exception:
            pass

# -------------------------
# ENTRY POINT
# -------------------------
if __name__ == "__main__":
    run()
