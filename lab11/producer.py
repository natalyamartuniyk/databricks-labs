import logging
import os
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

NUM_EVENTS = 25
NUM_RETRIES = 5
CATEGORIES = ["Music", "Gaming", "News", "Education", "Entertainment"]


def generate_events(n):
    """Create events once. event_id is assigned at creation and never changes."""
    events = []
    for i in range(n):
        events.append({
            "event_id": str(uuid.uuid4()),
            "video_id": f"VID_{1000 + i}",
            "user_id": f"USER_{500 + (i % 10)}",
            "category": CATEGORIES[i % len(CATEGORIES)],
            "watch_duration_sec": 45 + i * 7,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
        })
    return events


def send(stream, events, label):
    for event in events:
        offset = stream.ingest_record_offset(event)
        logger.debug("[%s] offset=%s event_id=%s", label, offset, event["event_id"])
    logger.info("[%s] sent %d records", label, len(events))


def main():
    events = generate_events(NUM_EVENTS)
    logger.info("Generated %d events for table %s", len(events), TABLE_NAME)

    sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
    stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, TableProperties(TABLE_NAME))
    logger.info("Zerobus stream opened")

    try:
        send(stream, events, "initial")
        send(stream, events[:NUM_RETRIES], "retry")
    except ZerobusException:
        logger.exception("Zerobus ingestion failed")
        raise
    finally:
        stream.close()
        logger.info("Zerobus stream closed")

    logger.info(
        "Done: %d unique events + %d retries = %d records",
        NUM_EVENTS, NUM_RETRIES, NUM_EVENTS + NUM_RETRIES,
    )


if __name__ == "__main__":
    main()