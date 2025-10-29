#!/usr/bin/env python3
"""
OracleDB version of 'common items finder'.
Efficiently identifies items common across a minimum number of stores.
Chunked fetching + timing + resilient Oracle connection.
"""

import os
import time
import logging
import oracledb
import pandas as pd
import yaml
from tqdm import tqdm
from logging.handlers import RotatingFileHandler
from collections import defaultdict

# ----------------------------
# StepTimer for performance tracking
# ----------------------------
class StepTimer:
    def __init__(self):
        self.timings = {}

    def start(self, name):
        self.timings[name] = {"start": time.time(), "end": None}

    def end(self, name):
        self.timings[name]["end"] = time.time()

    def summary(self, logger):
        logger.info("=== Step Execution Times ===")
        for k, v in self.timings.items():
            if v["start"] and v["end"]:
                logger.info(f"{k:<40} {v['end'] - v['start']:.2f} sec")
        logger.info("============================")


# ----------------------------
# Logging setup
# ----------------------------
def setup_logging(log_dir, max_bytes_mb=20, backups=5):
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "app.log")

    logger = logging.getLogger("OracleCommonItems")
    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(log_file, maxBytes=max_bytes_mb * 1024 * 1024, backupCount=backups)
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(fmt))
    logger.addHandler(console)
    return logger


# ----------------------------
# Load config
# ----------------------------
def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ----------------------------
# Oracle connection with retry
# ----------------------------
def get_oracle_connection(cfg, logger):
    dsn = cfg["oracle"]["dsn"]
    user = cfg["oracle"]["user"]
    password = cfg["oracle"]["password"]
    max_retries = cfg["params"].get("max_retries", 3)
    retry_delay = cfg["params"].get("retry_delay_sec", 5)

    for attempt in range(1, max_retries + 1):
        try:
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
            logger.info("✅ Connected to Oracle successfully.")
            return conn
        except Exception as e:
            logger.warning(f"Attempt {attempt} failed: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} sec...")
                time.sleep(retry_delay)
            else:
                logger.error("❌ All retries failed. Exiting.")
                raise


# ----------------------------
# Fetch Active Items
# ----------------------------
def fetch_active_items(conn, limit, logger):
    cursor = conn.cursor()
    sql = "SELECT item FROM item_master WHERE status = 'A'"
    if limit and limit > 0:
        sql += f" FETCH FIRST {limit} ROWS ONLY"
    cursor.execute(sql)
    items = [row[0] for row in cursor]
    logger.info(f"Fetched {len(items)} active items")
    cursor.close()
    return set(items)


# ----------------------------
# Stream ITEM_LOC in chunks
# ----------------------------
def stream_item_loc(conn, chunk_size, active_items, logger):
    cursor = conn.cursor()
    sql = "SELECT item, loc FROM item_loc WHERE status <> 'I' AND loc_type = 'S'"
    cursor.execute(sql)

    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        filtered = [(i, l) for i, l in rows if i in active_items]
        yield filtered

    cursor.close()


# ----------------------------
# Main logic
# ----------------------------
def find_common_items(cfg):
    logger = setup_logging(
        cfg["paths"]["log_dir"],
        max_bytes_mb=cfg["logging"].get("log_file_size_mb", 20),
        backups=cfg["logging"].get("backups", 5),
    )
    timer = StepTimer()
    timer.start("Total runtime")

    conn = get_oracle_connection(cfg, logger)

    chunk_size = cfg["params"]["chunk_size"]
    min_store_count = cfg["params"]["min_store_count"]
    item_limit = cfg["params"]["item_limit"]
    active_item_limit = cfg["params"].get("active_item_limit", 0)
    output_dir = cfg["paths"]["output_dir"]

    timer.start("Fetch active items")
    active_items = fetch_active_items(conn, active_item_limit, logger)
    timer.end("Fetch active items")

    timer.start("Stream item_loc and build store->items")
    store_items = defaultdict(set)
    total_rows = 0

    for rows in tqdm(stream_item_loc(conn, chunk_size, active_items, logger), desc="Processing ITEM_LOC"):
        total_rows += len(rows)
        for item, loc in rows:
            store_items[loc].add(item)

    timer.end("Stream item_loc and build store->items")
    logger.info(f"Processed total {total_rows} ITEM_LOC rows")
    logger.info(f"Total stores found: {len(store_items)}")

    timer.start("Select top stores and intersect")
    store_counts = sorted(((loc, len(items)) for loc, items in store_items.items()), key=lambda x: x[1], reverse=True)
    top_stores = [loc for loc, _ in store_counts[:min_store_count]]

    # progressive intersection
    store_sets = [(loc, store_items[loc]) for loc in top_stores]
    store_sets.sort(key=lambda x: len(x[1]))

    intersection = None
    for idx, (loc, sset) in enumerate(store_sets, 1):
        intersection = sset if intersection is None else intersection & sset
        logger.info(f"After {idx} stores, intersection size = {len(intersection)}")
        if len(intersection) < item_limit:
            logger.warning("Intersection smaller than item_limit, stopping early.")
            break

    timer.end("Select top stores and intersect")

    if not intersection or len(intersection) < item_limit:
        logger.error("Failed to find enough common items. Check data distribution.")
        timer.end("Total runtime")
        timer.summary(logger)
        return

    final_items = sorted(list(intersection))[:item_limit]
    final_stores = sorted(top_stores)[:min_store_count]

    timer.start("Write final dataset")
    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(output_dir, "final_store_item.csv")

    with open(out_file, "w", encoding="utf-8") as f:
        f.write("STORE,ITEM\n")
        for store in final_stores:
            for item in final_items:
                f.write(f"{store},{item}\n")

    logger.info(f"✅ Output written: {out_file} ({len(final_stores) * len(final_items)} rows)")
    timer.end("Write final dataset")

    timer.end("Total runtime")
    timer.summary(logger)
    logger.info("=== Process Complete ===")
    conn.close()


# ----------------------------
# Entrypoint
# ----------------------------
if __name__ == "__main__":
    cfg = load_config("config.yaml")
    find_common_items(cfg)
