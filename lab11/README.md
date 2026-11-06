# Lab 11 — Zero-Bus Streaming with Zerobus Ingest

## Goal

Push YouTube watch events directly into a Unity Catalog Delta table with **Zerobus Ingest**, without a message bus in between, make the downstream processing **idempotent**, and compare this design with the **Event Hub (Kafka-compatible)** pipeline built in Lab 3.

## Architecture

**Lab 3 — bus-based:**
```
Python producer → Azure Event Hub → Spark Structured Streaming (readStream) → eventhub_youtube_bronze
```

**Lab 11 — zero-bus:**
```
Python producer (local) → Zerobus Ingest endpoint → zerobus_youtube_bronze
                                                         ↓  MERGE on event_id
                                                   zerobus_youtube_silver
```

Both pipelines use the **same event schema** (`event_id`, `video_id`, `user_id`, `category`, `watch_duration_sec`, `event_timestamp`), so the comparison is made on identical data.

The producer runs **outside Databricks** as a standalone Python application. This reflects the zero-bus idea directly: an external application pushes events straight into the lakehouse, with no cluster, consumer job or notebook on the ingestion path.

## Setup

* **Target table:** `dbr_dev_ua5816bd.natalkamartinuk55_bronze.zerobus_youtube_bronze` — managed Delta table, created in advance; Zerobus never creates or evolves the table schema.
* **Authentication:** dedicated service principal `natalkamartinuk55-zerobus-sp` (OAuth M2M, client ID + secret), with the *Workspace access* entitlement. The SDK exchanges the credentials for a short-lived OAuth token (valid for 1 hour).
* **Least privilege:** the service principal has only `USE CATALOG`, `USE SCHEMA` and `MODIFY, SELECT` on the single bronze table.
* **Secrets:** client ID/secret and endpoints are stored in a local `.env` file, excluded from git via `.gitignore`.

## Files

| File | Purpose |
|---|---|
| `01_setup_tables` | Creates the bronze table and grants privileges to the service principal |
| `test_connection.py` | Sends a single test record to verify authentication and connectivity |
| `producer.py` | Streams 100 events at a fixed rate via Zerobus with explicit acknowledgements, then re-sends 5 of them to simulate retries |
| `02_idempotent_silver` | Duplicate checks on bronze, creates silver, idempotent `MERGE`, re-run and late-duplicate tests |
| `03_validation` | End-to-end latency for Zerobus and for the Event Hub pipeline from Lab 3 |

## Producer

* **Stream first, events second.** The Zerobus stream (OAuth token + gRPC connection) is opened before any event is created, so connection setup is not counted as event latency.
* **Paced generation.** 100 events are generated at 5 events/second (~20 s). Each event gets `event_timestamp` at the moment it is created.
* **Stable `event_id`.** A UUID is assigned once, when the event is created, and reused on every resend. This is what makes duplicates detectable downstream.
* **Explicit acknowledgements.** Records are enqueued with `ingest_record_offset()`; after every 10 events the producer blocks on `wait_for_offset()` for the last offset of the chunk, which confirms that everything up to that offset is durable. At the end, `flush()` waits for all remaining records (retries and any tail).
* **Retry simulation.** The first 5 events are re-sent with the same `event_id`.
* **Metrics and logging.** Counters for sent / retried / acknowledged records and ack wait times; logging via the `logging` module, unrecoverable SDK errors (`ZerobusException`) are logged with traceback and re-raised.

Producer run summary:

```
Summary: sent=100 retried=5 acked=105 | ack wait avg=226 ms max=363 ms
```

The ack wait is the time spent blocked in `wait_for_offset()`, not the full send-to-ack time: while the producer was sending the next events, earlier acknowledgements were already arriving.

## Idempotent processing

Zerobus Ingest provides **at-least-once** delivery: a record can be persisted while its acknowledgement is lost, and the producer re-sends it. Bronze therefore keeps duplicates; deduplication happens on the bronze → silver step.

`MERGE` into silver protects against both kinds of duplicates:

* **Duplicates within one batch** (original and retry both in bronze, neither in silver yet) — removed in the source subquery with `QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp) = 1`.
* **Late duplicates** (retry arrives after the original is already in silver) — filtered by `ON t.event_id = s.event_id` + `WHEN NOT MATCHED THEN INSERT`.

There is no `WHEN MATCHED` branch: events are immutable, so an event already in silver is left untouched.

### Results

| Check | Result |
|---|---|
| Bronze after producer run | 105 rows / 100 unique `event_id` (5 duplicates) |
| `MERGE` — first run | `num_inserted_rows = 100` |
| Silver after first run | 100 rows / 100 unique |
| `MERGE` — re-run, no new data | `num_inserted_rows = 0` |
| Late duplicate inserted into bronze (copy of `VID_1010`), `MERGE` run again | `num_inserted_rows = 0` |
| Silver after all runs | 100 rows / 100 unique |

The late retry was simulated by inserting a copy of an existing bronze row with SQL (bypassing Zerobus), since only the presence of a second copy in bronze matters for the test.

## Latency

**Latency** here is the time from event creation (`event_timestamp`) to the moment the row became visible in the Delta table.

* **Zerobus:** the landing time is taken from the hidden `_metadata.file_modification_time` column (when the data file containing the row was written). Only the first arrival of each `event_id` is counted, so retries and the late duplicate do not inflate the numbers.
* **Event Hub (Lab 3):** the bronze table has three timestamps, so the latency splits into two stages: event → broker (`_eventhub_enqueued_timestamp`) and broker → table (`_ingestion_timestamp`, written by the consumer).

| Pipeline | Events | avg | p50 | p95 | max |
|---|---|---|---|---|---|
| Zerobus (Lab 11) | 100 | 2.65 s | 2.64 s | 4.85 s | 5.08 s |

| Pipeline | Events | event → broker | broker → table | end-to-end (avg) |
|---|---|---|---|---|
| Event Hub (Lab 3) | 125 | 0.56 s | 19.61 s | 20.17 s |

Observations:

* In the Event Hub pipeline almost all of the latency is **waiting in the broker** until the consumer picks events up. In Lab 3 the consumer (`availableNow` trigger) was started manually right after the producer; with a less frequent schedule this stage grows to minutes or hours. A continuous trigger would lower it, but requires an always-on cluster.
* In the zero-bus pipeline there is no consumer to wait for: events become queryable within a few seconds.
* A first measurement with all events created **before** opening the stream gave ~7.9 s, because OAuth token minting and connection setup were counted as event latency. Opening the stream first removed this from the measurement.
* Both measurements are small samples on a shared development workspace — they show the order of magnitude, not a benchmark.

## Kafka-style vs. zero-bus

| Dimension | Event Hub (Lab 3) | Zerobus Ingest (Lab 11) |
|---|---|---|
| **Components** | Producer, Event Hub namespace, Spark consumer job, Delta table | Producer, Delta table |
| **Sinks** | Multi-sink: any number of independent consumer groups can read the same events | Single-sink: the lakehouse table only |
| **Decoupling** | Strong — producer only knows the topic, consumers can be added without touching it | Weaker — producer is bound to a specific table and its schema |
| **Replay** | Built-in: events retained in the broker, re-readable from any offset | Not in the service itself; history lives in the Delta table (time travel) |
| **Latency (measured)** | ~20 s end-to-end, almost all of it waiting for the consumer | ~2.6 s median to queryable row |
| **State to manage** | Offsets, consumer group, `checkpointLocation` | None on the ingestion path |
| **Authentication** | Event Hub connection string (SAS key) in a secret scope | Service principal OAuth (client ID + secret), UC grants on one table |
| **Schema handling** | Payload is a raw string; schema applied by the consumer (`from_json`) | Table schema is the contract; non-matching records are rejected at ingestion |
| **Delivery semantics** | At-least-once into the broker; Spark → Delta is exactly-once via checkpoints | At-least-once (confirmed per offset); deduplication required downstream |
| **Cost model** | Broker billed continuously + compute for the consumer job | Serverless, usage-based; no broker or consumer compute |
| **Operational load** | Broker provisioning and monitoring, consumer job scheduling, checkpoint maintenance | Service principal, grants, and keeping producer and table schemas in sync |
