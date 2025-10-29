#!/usr/bin/env python3
"""
Async + Chunked implementation to find N items common across M stores.
Requires aioodbc and pyarrow/pandas for optional parquet output.

pip install aioodbc pandas pyyaml tqdm pyarrow
"""

import os
import time
import asyncio
import logging
import yaml
import aioodbc
import pandas as pd
from collections import defaultdict
from tqdm.asyncio import tqdm as async_tqdm
from logging.handlers import RotatingFileHandler

# ----------------------------
# Step Timer for timing steps
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
            start, end = v["start"], v["end"]
            if start is None or end is None:
                continue
            dur = end - start
            logger.info(f"{k:<40} {dur:8.2f} sec")
        logger.info("============================")


# ----------------------------
# Logging setup
# ----------------------------
def setup_logging(log_dir, max_bytes_mb=20, backups=5):
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "app.log")

    logger = logging.getLogger("AsyncCommonItems")
    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(log_file, maxBytes=max_bytes_mb * 1024 * 1024, backupCount=backups)
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)

    # console handler
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(fmt))
    logger.addHandler(ch)
    return logger


# ----------------------------
# Load config
# ----------------------------
def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ----------------------------
# Async DB helpers (aioodbc)
# ----------------------------
async def get_pool(dsn, user, password, maxsize=5, logger=None):
    """
    Create a connection pool for small concurrent queries.
    Ensures maxsize >= minsize and gracefully falls back if pool creation fails.
    """
    try:
        minsize = 1
        if maxsize < minsize:
            if logger:
                logger.warning(f"Invalid pool size (maxsize={maxsize}), adjusting to 1")
            maxsize = 1

        pool = await aioodbc.create_pool(
            dsn=dsn,
            user=user,
            password=password,
            autocommit=True,
            minsize=minsize,
            maxsize=maxsize,
        )
        if logger:
            logger.info(f"Connection pool created successfully (minsize={minsize}, maxsize={maxsize})")
        return pool

    except Exception as e:
        if logger:
            logger.error(f"Pool creation failed: {e}. Falling back to direct async connection mode.")

        class DummyPool:
            """
            Fallback class when aioodbc pool creation fails.
            Provides async acquire() and release() compatible with 'async with'.
            """

            def __init__(self, dsn, user, password):
                self.dsn = dsn
                self.user = user
                self.password = password

            class _ConnectionWrapper:
                def __init__(self, conn):
                    self.conn = conn
                async def __aenter__(self):
                    return self.conn
                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    await self.conn.close()

            async def acquire(self):
                conn = await aioodbc.connect(dsn=self.dsn, user=self.user, password=self.password, autocommit=True)
                return self._ConnectionWrapper(conn)

        return DummyPool(dsn, user, password)




async def fetch_active_items(pool, active_item_limit=0, logger=None):
    """
    Fetch active items from item_master (synchronous small result).
    Works with both aioodbc.Pool and DummyPool.
    """
    async with (await pool.acquire()) as conn:
        async with conn.cursor() as cur:
            sql = "SELECT item FROM item_master WHERE status = 'A'"
            if active_item_limit and active_item_limit > 0:
                sql += f" FETCH FIRST {active_item_limit} ROWS ONLY"
            await cur.execute(sql)
            rows = await cur.fetchall()
            items = [r[0] for r in rows]
            if logger:
                logger.info(f"Active items fetched: {len(items)}")
            return set(items)


async def stream_item_loc_rows(dsn, user, password, chunk_size, logger):
    """
    Stream item_loc rows (only loc_type='S' and status<>'I').
    We open a connection and cursor and use fetchmany in async loop to yield row batches.
    Each row is (item, loc).
    IMPORTANT: The yielded rows are raw tuples; process them immediately to avoid holding them.
    """
    conn = await aioodbc.connect(dsn=dsn, user=user, password=password, autocommit=True)
    cur = await conn.cursor()
    # minimal select — don't join item_master in SQL; we'll filter with active_items set client-side
    sql = "SELECT item, loc FROM item_loc WHERE status <> 'I' AND loc_type = 'S'"
    await cur.execute(sql)

    while True:
        rows = await cur.fetchmany(chunk_size)
        if not rows:
            break
        yield rows

    await cur.close()
    await conn.close()


# ----------------------------
# High-level orchestrator
# ----------------------------
async def find_common_items_async(cfg):
    logger = setup_logging(cfg["paths"]["log_dir"],
                           max_bytes_mb=cfg["logging"].get("log_file_size_mb", 20),
                           backups=cfg["logging"].get("backups", 5))
    timer = StepTimer()
    timer.start("Total runtime")
    logger.info("=== Starting Async Common Items Finder ===")

    dsn = cfg["odbc"]["dsn"]
    user = cfg["odbc"]["user"]
    password = cfg["odbc"]["password"]

    chunk_size = cfg["params"]["chunk_size"]
    min_store_count = cfg["params"]["min_store_count"]
    item_limit = cfg["params"]["item_limit"]
    active_item_limit = cfg["params"].get("active_item_limit", 0)
    output_dir = cfg["paths"]["output_dir"]

    # create pool for small queries
    pool = await get_pool(dsn, user, password, maxsize=min(cfg["params"].get("max_concurrency", 8), 16))

    # Step 1: active items (small)
    timer.start("Fetch active items")
    active_items = await fetch_active_items(pool, active_item_limit, logger)
    timer.end("Fetch active items")

    if not active_items:
        logger.error("No active items found. Exiting.")
        return

    # Step 2: stream item_loc to build store -> set(items)
    timer.start("Stream item_loc and build store->items map")
    store_items = defaultdict(set)   # store -> set(items)
    total_rows = 0
    # iterate asynchronously over generator
    async for rows in stream_item_loc_rows(dsn, user, password, chunk_size, logger):
        total_rows += len(rows)
        # process rows quickly: filter by active items to reduce work
        for item, loc in rows:
            if item in active_items:
                store_items[loc].add(item)

        # optional logging per chunk
        if total_rows % (chunk_size * 10) == 0:
            logger.info(f"Processed ~{total_rows} item_loc rows")

    timer.end("Stream item_loc and build store->items map")
    logger.info(f"Total item_loc rows streamed: {total_rows}")
    logger.info(f"Total distinct stores discovered: {len(store_items)}")

    if len(store_items) == 0:
        logger.error("No stores found in item_loc. Exiting.")
        return

    # Step 3: select candidate stores - top stores by number of items
    timer.start("Select top stores by item counts")
    # build list of (store, count) and sort desc
    store_counts = [(loc, len(items)) for loc, items in store_items.items()]
    store_counts.sort(key=lambda x: x[1], reverse=True)

    # pick top K (we try to pick at least min_store_count stores; if not enough, pick all)
    K = min(len(store_counts), min_store_count)
    top_stores = [loc for loc, cnt in store_counts[:K]]
    logger.info(f"Selected top {len(top_stores)} stores (by item count). Example top counts: {store_counts[:5]}")
    timer.end("Select top stores by item counts")

    # Step 4: compute intersection across chosen stores
    timer.start("Compute intersection of items across selected stores")
    # We'll intersect sets progressively, starting from smallest store set in the top_stores (to intersect faster)
    # Build list of sets for top_stores
    store_sets = [(loc, store_items[loc]) for loc in top_stores]
    # sort ascending by set size (so intersection reduces quickly)
    store_sets.sort(key=lambda x: len(x[1]))

    # progressive intersection
    if not store_sets:
        logger.error("No top stores to intersect. Exiting.")
        return

    intersection_set = None
    iter_count = 0
    for loc, sset in store_sets:
        iter_count += 1
        if intersection_set is None:
            intersection_set = set(sset)  # copy
        else:
            # intersect
            intersection_set &= sset

        logger.info(f"After intersecting {iter_count} stores, remaining common items: {len(intersection_set)}")
        # early stop if intersection smaller than item_limit (can't get back bigger)
        if len(intersection_set) < item_limit:
            logger.warning("Intersection dropped below required item_limit during progressive intersection.")
            break

    timer.end("Compute intersection of items across selected stores")

    # Step 5: evaluate intersection result and produce final sets
    timer.start("Evaluate & prepare final selections")
    final_items = []
    final_stores = []

    if intersection_set and len(intersection_set) >= item_limit:
        # we have at least item_limit items common across these top stores
        final_items = list(intersection_set)[:item_limit]
        # final stores are top_stores (we need exactly min_store_count)
        final_stores = top_stores[:min_store_count]
        logger.info(f"SUCCESS: Found {len(final_items)} items common across {len(final_stores)} stores")
    else:
        # Try a fallback: attempt to find ANY set of min_store_count stores that produce at least item_limit intersection.
        # This is potentially expensive; we try a greedy approach:
        logger.warning("Primary intersection failed. Trying greedy fallback: choose combination of stores iteratively.")
        # Greedy strategy: start with the store that has largest item count, iteratively add stores that least reduce intersection.
        # Start with store with largest set:
        greedy_stores = [store_counts[0][0]]  # store with most items
        greedy_intersection = set(store_items[greedy_stores[0]])
        idx = 1
        while len(greedy_stores) < min_store_count and idx < len(store_counts):
            cand_loc = store_counts[idx][0]
            cand_set = store_items[cand_loc]
            # compute new intersection size quickly
            new_inter = greedy_intersection & cand_set
            # choose it
            greedy_stores.append(cand_loc)
            greedy_intersection = new_inter
            logger.info(f"Greedy add store {cand_loc}: intersection size now {len(greedy_intersection)} (stores chosen: {len(greedy_stores)})")
            idx += 1
            if len(greedy_intersection) < item_limit and idx % 50 == 0:
                # optional early break if hopeless; but we continue to try until we picked min_store_count stores
                logger.debug("Greedy progress: intersection below item_limit, but will continue until chosen stores count reached.")

        if len(greedy_intersection) >= item_limit and len(greedy_stores) >= min_store_count:
            final_items = list(greedy_intersection)[:item_limit]
            final_stores = greedy_stores[:min_store_count]
            logger.info("Fallback greedy succeeded.")
        else:
            logger.error("Unable to find required item_limit items that are common across min_store_count stores.")
            # log helpful diagnostic info
            logger.info(f"Max intersection size observed: {len(intersection_set) if intersection_set else 0}")
            # Save some diagnostics to disk
            diag_path = os.path.join(output_dir, "diagnostics")
            os.makedirs(diag_path, exist_ok=True)
            pd.Series({loc: len(items) for loc, items in store_items.items()}).sort_values(ascending=False).to_csv(os.path.join(diag_path, "store_item_counts.csv"))
            logger.info(f"Diagnostics written to {diag_path}")
            timer.end("Evaluate & prepare final selections")
            timer.end("Total runtime")
            timer.summary(logger)
            return  # exit early as requirement cannot be satisfied with given data

    timer.end("Evaluate & prepare final selections")

    # Step 6: Build final cross-joined dataset and write CSV (or parquet)
    timer.start("Build and write final dataset")
    os.makedirs(output_dir, exist_ok=True)
    rows = []
    # create deterministic order
    final_items_sorted = sorted(final_items)
    final_stores_sorted = sorted(final_stores)

    # produce rows sequentially and write in streamed fashion to avoid big memory usage
    out_csv = os.path.join(output_dir, "final_store_item.csv")
    # write header first
    with open(out_csv, "w", encoding="utf-8") as f:
        f.write("STORE,ITEM\n")
        for store in final_stores_sorted:
            for item in final_items_sorted:
                f.write(f"{store},{item}\n")

    logger.info(f"Final CSV written to {out_csv} (rows={len(final_stores_sorted) * len(final_items_sorted)})")
    timer.end("Build and write final dataset")

    timer.end("Total runtime")
    timer.summary(logger)
    logger.info("=== Completed ===")


# ----------------------------
# Entrypoint wrapper
# ----------------------------
def main():
    cfg = load_config()
    asyncio.run(find_common_items_async(cfg))


if __name__ == "__main__":
    main()
