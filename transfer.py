import os
import sys
import io
import math
import time
import logging
import asyncio
import concurrent.futures
import pandas as pd
import oracledb
from logging.handlers import RotatingFileHandler
from datetime import datetime

# ==============================================================
# 🔧 GLOBAL UTF-8 FIX (Handles Windows charmaps and emojis)
# ==============================================================
os.environ["PYTHONIOENCODING"] = "utf-8"
os.environ["PYTHONUTF8"] = "1"

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ==============================================================
# 🧭 CONFIGURATION
# ==============================================================
DB_CONFIG = {
    "user": "your_user",
    "password": "your_pass",
    "dsn": "your_tns_entry_or_host/service",
    "encoding": "UTF-8"
}

CHUNK_SIZE = 100_000         # Rows per chunk (tune based on memory)
MAX_WORKERS = os.cpu_count() or 8  # Parallel processes
LOG_DIR = "logs"
OUTPUT_DIR = "output"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==============================================================
# 🧾 LOGGING SETUP
# ==============================================================
def setup_logging():
    log_file = os.path.join(LOG_DIR, "oracle_query.log")
    logger = logging.getLogger("OracleQuery")
    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(log_file, maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8")
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(fmt))
    logger.addHandler(console)

    return logger

logger = setup_logging()

# ==============================================================
# ⚙️ CORE QUERY EXECUTION
# ==============================================================

ACTIVE_ITEMS_QUERY = """
SELECT im.item, il.loc, il.loc_type, il.status
FROM item_master im
JOIN item_loc il ON im.item = il.item
WHERE im.status = 'A'
  AND il.status <> 'I'
"""

COUNT_QUERY = f"SELECT COUNT(*) FROM ({ACTIVE_ITEMS_QUERY})"

# ==============================================================
# 🧩 FUNCTIONS
# ==============================================================

def get_connection():
    return oracledb.connect(**DB_CONFIG)

def get_total_count():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(COUNT_QUERY)
            (count,) = cur.fetchone()
            return count

def fetch_chunk(offset, limit):
    """Fetch a chunk of data from Oracle"""
    try:
        start_time = time.time()
        conn = get_connection()
        cur = conn.cursor()

        paginated_query = f"""
        SELECT item, loc, loc_type, status FROM (
            SELECT im.item, il.loc, il.loc_type, il.status,
                   ROW_NUMBER() OVER (ORDER BY im.item, il.loc) rn
            FROM item_master im
            JOIN item_loc il ON im.item = il.item
            WHERE im.status = 'A'
              AND il.status <> 'I'
        )
        WHERE rn BETWEEN {offset + 1} AND {offset + limit}
        """

        cur.execute(paginated_query)
        rows = cur.fetchall()
        duration = time.time() - start_time
        logger.info(f"Chunk {offset // limit + 1}: fetched {len(rows)} rows in {duration:.2f}s ✅")
        cur.close()
        conn.close()
        return rows

    except Exception as e:
        logger.error(f"Error fetching chunk offset {offset}: {e}", exc_info=True)
        return []

async def fetch_all_chunks(total_rows):
    """Parallel chunk fetching using process pool"""
    total_chunks = math.ceil(total_rows / CHUNK_SIZE)
    logger.info(f"Total rows: {total_rows:,}, Chunks: {total_chunks}, Workers: {MAX_WORKERS}")

    results = []
    loop = asyncio.get_running_loop()

    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [
            loop.run_in_executor(executor, fetch_chunk, offset, CHUNK_SIZE)
            for offset in range(0, total_rows, CHUNK_SIZE)
        ]
        for i, task in enumerate(asyncio.as_completed(tasks), 1):
            chunk = await task
            if chunk:
                results.extend(chunk)
            logger.info(f"✅ Completed chunk {i}/{total_chunks}")

    return results

# ==============================================================
# 🚀 MAIN EXECUTION
# ==============================================================

async def main():
    start_time = datetime.now()
    logger.info("🚀 Starting active item fetch...")

    total_rows = get_total_count()
    logger.info(f"🔢 Total eligible rows: {total_rows:,}")

    data = await fetch_all_chunks(total_rows)
    logger.info(f"✅ Completed fetching {len(data):,} rows in total")

    df = pd.DataFrame(data, columns=["ITEM", "LOC", "LOC_TYPE", "STATUS"])
    output_file = os.path.join(OUTPUT_DIR, f"active_items_{datetime.now():%Y%m%d_%H%M%S}.csv")
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    logger.info(f"💾 Data saved to {output_file}")

    total_duration = (datetime.now() - start_time).total_seconds() / 60
    logger.info(f"🏁 Total execution time: {total_duration:.2f} minutes")

if __name__ == "__main__":
    asyncio.run(main())
