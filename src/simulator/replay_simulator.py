"""Replay simulator for the fraud detection pipeline.

Reads the PaySim CSV in its original row order and emits it as timed
batches, landing each batch as a JSON-lines file. This stands in for a
live transaction feed.

Landing target:
  - If DATABRICKS_HOST / DATABRICKS_TOKEN / UC_VOLUME_PATH are set, batches
    are uploaded to that Unity Catalog Volume path via the Databricks SDK.
  - Otherwise, batches are written to a local `data/landing/` directory,
    which is useful for testing the simulator itself before a workspace
    is wired up.

Run: python -m src.simulator.replay_simulator
"""

from __future__ import annotations

import io
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("replay_simulator")

load_dotenv()

PAYSIM_CSV_PATH = os.environ.get("PAYSIM_CSV_PATH", "data/raw/paysim.csv")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
BATCH_INTERVAL_SECONDS = float(os.environ.get("BATCH_INTERVAL_SECONDS", "30"))
UC_VOLUME_PATH = os.environ.get("UC_VOLUME_PATH")
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

LOCAL_LANDING_DIR = Path("data/landing")


def _get_databricks_client():
    """Return a WorkspaceClient if credentials are configured, else None."""
    if not (DATABRICKS_HOST and DATABRICKS_TOKEN and UC_VOLUME_PATH):
        return None
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)


def _batch_to_ndjson(batch: pd.DataFrame) -> bytes:
    """Convert a batch of rows to newline-delimited JSON bytes."""
    lines = batch.to_json(orient="records", lines=True)
    return lines.encode("utf-8")


def _emit_batch(client, batch: pd.DataFrame, batch_index: int) -> None:
    payload = _batch_to_ndjson(batch)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    filename = f"paysim_batch_{ts}_{batch_index:06d}.jsonl"

    if client is not None:
        remote_path = f"{UC_VOLUME_PATH.rstrip('/')}/{filename}"
        client.files.upload(remote_path, io.BytesIO(payload), overwrite=True)
        logger.info("Uploaded batch %d (%d rows) -> %s", batch_index, len(batch), remote_path)
    else:
        LOCAL_LANDING_DIR.mkdir(parents=True, exist_ok=True)
        local_path = LOCAL_LANDING_DIR / filename
        local_path.write_bytes(payload)
        logger.info("Wrote batch %d (%d rows) -> %s (local fallback)", batch_index, len(batch), local_path)


def run(loop: bool = False) -> None:
    csv_path = Path(PAYSIM_CSV_PATH)
    if not csv_path.exists():
        raise FileNotFoundError(
            f"PaySim CSV not found at {csv_path}. Download it from Kaggle "
            "(dataset 'ealaxi/paysim1') and place it there, or set "
            "PAYSIM_CSV_PATH in your .env."
        )

    client = _get_databricks_client()
    if client is None:
        logger.warning(
            "DATABRICKS_HOST/DATABRICKS_TOKEN/UC_VOLUME_PATH not fully set — "
            "writing batches to %s instead of a Unity Catalog Volume.",
            LOCAL_LANDING_DIR,
        )

    logger.info(
        "Starting replay: batch_size=%d interval=%ss source=%s",
        BATCH_SIZE,
        BATCH_INTERVAL_SECONDS,
        csv_path,
    )

    batch_index = 0
    while True:
        reader = pd.read_csv(csv_path, chunksize=BATCH_SIZE)
        for batch in reader:
            _emit_batch(client, batch, batch_index)
            batch_index += 1
            time.sleep(BATCH_INTERVAL_SECONDS)
        if not loop:
            break
        logger.info("Reached end of source data, looping back to start.")

    logger.info("Replay finished after %d batches.", batch_index)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Replay PaySim as a live batch feed.")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Restart from the beginning of the CSV when it's exhausted, "
        "instead of stopping.",
    )
    args = parser.parse_args()

    try:
        run(loop=args.loop)
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
