import logging
import os
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import TableProperties, ZerobusException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("producer")

load_dotenv()

SERVER_ENDPOINT = os.environ["ZEROBUS_SERVER_ENDPOINT"]
WORKSPACE_URL = os.environ["DATABRICKS_WORKSPACE_URL"]
TABLE_NAME = os.environ["ZEROBUS_TABLE_NAME"]
CLIENT_ID = os.environ["DATABRICKS_CLIENT_ID"]
CLIENT_SECRET = os.environ["DATABRICKS_CLIENT_SECRET"]

NUM_EVENTS = 100
EVENTS_PER_SECOND = 5
CHUNK_SIZE = 10
NUM_RETRIES = 5
CATEGORIES = ["Music", "Gaming", "News", "Education", "Entertainment"]


def make_event(i):
    """Create one event. event_id is assigned at creation and never changes."""
    return {
        "event_id": str(uuid.uuid4()),
        "video_id": f"VID_{1000 + i}",
        "user_id": f"USER_{500 + (i % 10)}",
        "category": CATEGORIES[i % len(CATEGORIES)],
        "watch_duration_sec": 45 + i * 7,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        }


def wait_for_ack(stream, offset, stats):
    """Block until everything up to `offset` is durable; record how long we waited."""
    started = time.perf_counter()
    stream.wait_for_offset(offset)
    wait_ms = (time.perf_counter() - started) * 1000
    stats["ack_waits_ms"].append(wait_ms)
    return wait_ms


def main():
    stats = {"sent": 0, "retried": 0, "acked": 0, "ack_waits_ms": []}
    sent_events = []

    sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
    stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, TableProperties(TABLE_NAME))
    logger.info("Zerobus stream opened for %s", TABLE_NAME)

    try:
        for i in range(NUM_EVENTS):
            event = make_event(i)
            offset = stream.ingest_record_offset(event)
            sent_events.append(event)
            stats["sent"] += 1

            if stats["sent"] % CHUNK_SIZE == 0:
               wait_ms = wait_for_ack(stream, offset, stats)
               stats["acked"] = stats["sent"]
               logger.info(
                   "Acked up to offset %s (%d/%d events), waited %.0f ms",
                    offset, stats["acked"], NUM_EVENTS, wait_ms,
               )

            time.sleep(1 / EVENTS_PER_SECOND)

        for event in sent_events[:NUM_RETRIES]:
            stream.ingest_record_offset(event)
            stats["retried"] += 1
        logger.info("Re-sent %d events with the same event_id", stats["retried"])

        stream.flush()
        stats["acked"] = stats["sent"] + stats["retried"]
        logger.info("Flush complete: all pending records are durable")
    except ZerobusException:
        logger.exception("Zerobus ingestion failed")
        raise
    finally:
        stream.close()
        logger.info("Zerobus stream closed")

    waits = stats["ack_waits_ms"]
    avg_wait = sum(waits) / len(waits) if waits else 0.0
    max_wait = max(waits) if waits else 0.0
    logger.info(
        "Summary: sent=%d retried=%d acked=%d | ack wait avg=%.0f ms max=%.0f ms",
        stats["sent"], stats["retried"], stats["acked"], avg_wait, max_wait,
    )


if __name__ == "__main__":
    main()