import oracledb
import time
import os
from datetime import datetime

# =============================
# CONFIGURATION SECTION
# =============================

ORACLE_CONFIG = {
    "user": "your_username",
    "password": "your_password",
    "dsn": "your_tns_or_connect_string"  # e.g. "host:port/service_name"
}

LOG_DIR = "oracle_query_logs"
os.makedirs(LOG_DIR, exist_ok=True)

# =============================
# OPTIMIZED QUERY
# =============================

QUERY = """
WITH s2 AS (
    SELECT /*+ MATERIALIZE */
           loc
    FROM rms.item_loc
    GROUP BY loc
    HAVING COUNT(item) > 2000
    FETCH FIRST 240 ROWS ONLY
),
im AS (
    SELECT /*+ MATERIALIZE */
           i3.item AS al,
           i3.loc AS bl
    FROM rms.item_loc i3
    WHERE i3.loc_type = 'S'
      AND i3.status <> 'I'
      AND EXISTS (
          SELECT 1 FROM s2 WHERE s2.loc = i3.loc
      )
    GROUP BY i3.item, i3.loc
    HAVING COUNT(DISTINCT i3.loc) = 240
    FETCH FIRST 2000 ROWS ONLY
),
wh AS (
    SELECT /*+ MATERIALIZE */
           i2.item AS a
    FROM rms.item_loc i2
    WHERE i2.loc = 486
      AND i2.status = 'A'
)
SELECT /*+ USE_HASH(wh im) USE_HASH(im s2) PARALLEL(8) */
       im.al, im.bl
FROM wh
JOIN im ON wh.a = im.al
JOIN s2 ON im.bl = s2.loc
"""

# =============================
# EXECUTION AND LOGGING
# =============================

def log_message(message):
    """Write logs with timestamps."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(LOG_DIR, f"oracle_query_{timestamp}.log")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(message + "\n")
    print(message)

def run_query():
    connection = None
    try:
        log_message("🔗 Connecting to Oracle Database...")
        connection = oracledb.connect(**ORACLE_CONFIG)
        cursor = connection.cursor()

        # Force plan capture
        cursor.execute("EXPLAIN PLAN FOR " + QUERY)

        log_message("🚀 Executing query...")
        start_time = time.time()
        cursor.execute(QUERY)
        results = cursor.fetchall()
        duration = time.time() - start_time

        log_message(f"✅ Query executed successfully in {duration:.2f} seconds.")
        log_message(f"📊 Rows fetched: {len(results)}")

        # Capture and display execution plan
        cursor.execute("SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY())")
        plan = "\n".join(row[0] for row in cursor.fetchall())
        log_message("🧠 Execution Plan:\n" + plan)

    except Exception as e:
        log_message(f"❌ Error: {str(e)}")
    finally:
        if connection:
            connection.close()
            log_message("🔒 Connection closed.")

if __name__ == "__main__":
    run_query()
