# Lab 10 — External Operational Databases: Lakehouse Federation & CDC

An external PostgreSQL database (Neon, free tier, AWS eu-central-1) plays the role of an operational shop database.
Part A queries it in place from Databricks; Part B captures changes incrementally into an SCD Type 2 table.

| File | What it does |
|---|---|
| `neon_setup.sql` | Creates the source database in Neon: `sales.customers` (1,000 rows) and `sales.orders` (200,000 rows) with indexes. Run in Neon SQL Editor. |
| `part_a_federation` | Connection, foreign catalog, queries, pushdown, federation vs Delta copy, join of external and Delta tables. |
| `part_b_cdc` | Delta Change Data Feed source, incremental SCD2 pipeline with `MERGE INTO` and a version checkpoint. |

## How to reproduce

1. Create a Neon project, run `neon_setup.sql` in its SQL Editor. Use the **direct** host (connection pooling off): Federation connects over JDBC, and PgBouncer in transaction mode does not work well with it.
2. Store the Neon password as a Databricks secret — it never appears in code:
   ```bash
   databricks secrets create-scope lab10 --profile academy
   databricks secrets put-secret lab10 neon-password --profile academy   # value entered interactively
   ```
3. Run `part_a_federation`, then `part_b_cdc` (Run all is safe for both).

---

## Part A — Lakehouse Federation

`CREATE CONNECTION` (type `postgresql`, password via `secret('lab10', 'neon-password')`) + `CREATE FOREIGN CATALOG` mount the Neon database `neondb` into Unity Catalog as `natalia_neon_catalog`. Only metadata is stored; rows are read from Neon at query time. Connection and catalog names include my name because they are metastore-level objects shared by the whole workspace. `DESCRIBE CONNECTION` shows host, port, owner and `Read-only: true` — the password is never displayed.

What was checked:

- **Type mapping:** `VARCHAR(n)` → `string` (length limit lost), `NUMERIC(10,2)` → `decimal(10,2)`. Constraints and indexes are not visible in Unity Catalog — they keep working inside Neon only.
- **Live data:** a row inserted in Neon was visible in the next Databricks query (200,000 → 200,001) with no pipeline or refresh.
- **Pushdown:** `EXPLAIN FORMATTED` shows the SQL sent to Neon in `External engine query`. For the December revenue query both the filter (`order_date >= '2025-12-01'`) and the aggregation (`GROUP BY status`, `COUNT`, `SUM`) were pushed down, so 4 rows came back instead of 200,000. `round()` and `ORDER BY revenue` ran in Databricks: they depend on an expression that was not pushed, and once one step stays in Databricks everything above it does too. A plain `SELECT *` pushes nothing — the external query selects all five columns with no `WHERE`.
- **Join:** live `customers` from Neon joined with the Delta copy of `orders` (December revenue by city and segment, all 15 combinations). A join across two sources always runs in Databricks; only the needed customer columns are fetched from Neon.

### Discussion

**Federation vs ingestion.** The same query was slower through federation than on the Delta copy (`CREATE TABLE AS SELECT` took 14.7 s to ingest 200k rows). The difference stays moderate here because pushdown let Neon return only 4 rows; it grows with large tables, joins with big Delta tables and filters that cannot be pushed. In return, federation is always current: after a new order in Neon, federation returned 200,001 while the Delta copy stayed at 200,000 until the next full reload.

| | Federation | Ingestion |
|---|---|---|
| Freshness | live | snapshot at `_ingested_at` |
| Where the work runs | partly in the source (pushdown), rest in Databricks | fully in Databricks, Photon on Delta files |
| Load on the operational DB | every query | only at load time |
| Optimizer statistics | none (`sizeInBytes = 8.0 EiB` placeholder) | Delta file statistics |
| Fits | ad hoc checks, small or fast-changing reference data | repeated analytics, large tables, history |

**Latency.** Every federated query crosses the network to the source, so its time depends on the source: Neon free tier scales to zero, and the first query after a pause waits for a cold start. Delta queries do not depend on the source state.

**Security.** The password lives in a secret scope and inside the connection object, never in code or Git. The connection is read-only, so Databricks users cannot change operational data. Access is governed by Unity Catalog grants on the connection and catalog.

Two weak spots of this setup, both acceptable for a lab but not for production:

- The connection uses the database owner role (`neondb_owner`). Databricks marks the connection read-only, but the credential itself has full rights on the Neon database: if it leaked, it could drop or change anything. In production the connection would use a dedicated role with only `CONNECT` on the database, `USAGE` on the schema and `SELECT` on its tables (least privilege).
- Neon had no IP restrictions. In production the source would accept connections only from Databricks egress IPs or through a private network.

---

## Part B — Change Data Capture

```
Neon sales.customers ──(initial load via federation)──► customers_src  (Delta, CDF on)
                                                              │ table_changes(from checkpoint + 1)
                                                              ▼
                                                      dim_customers_scd2  (SCD Type 2)
```

Federation shows only the current state: intermediate updates and deletes are invisible through it, and log-based CDC from PostgreSQL would need extra tooling (Debezium, Lakeflow Connect). So `customers_src` acts as the CDC landing table — a Delta mirror of the source with Change Data Feed enabled from version 0.

**Pipeline — `run_cdc_batch()`**, called after every round of changes:

1. reads `last_processed_version` from `cdc_checkpoint` and the latest source version; if there is nothing new, it returns immediately (no MERGE, checkpoint unchanged);
2. reads only the new versions with `table_changes()`;
3. drops `update_preimage` and keeps one event per customer — the latest `_commit_version` (`MERGE` allows at most one source row per target row);
4. `MERGE INTO`: inserts and updates are fed twice. The copy with `merge_key = customer_id` only closes the current version if there is one; the copy with `merge_key = NULL` never matches and inserts the new version. A delete closes the current version and sets `is_deleted = true` (soft delete, history kept);
5. moves the checkpoint.

**Why inserts are fed twice too.** If a customer is deleted and re-created between two runs, only the insert survives deduplication, while the target still holds a current version (the delete never reached it). A keyed insert would match that row and, with no action for it, the event would be silently lost. Treating an insert like an update closes the old version and inserts the new one, so every case produces exactly one new current version: a brand-new key, a key returning after an earlier processed delete, and a key deleted and re-created within one batch. Customer 100 in round 2 tests this case.

**Scenario:**

| Round | Source changes | Result in the target |
|---|---|---|
| 1 | 7 moves, 15 changes segment, 42 deleted, 1001 inserted | 1002 versions, 1000 current, checkpoint 4 |
| 2 | 7 moves twice, 42 comes back, 100 deleted and re-created within the same batch | 1005 versions, 1001 current = 1001 in source, checkpoint 9 |
| 3 | no changes | function returns early, no MERGE, checkpoint stays 9 |

After round 2 the history is:

- customer 7 — Lutsk (v0, closed) → Kyiv (v1, closed) → Lviv (v6, current);
- customer 42 — Lutsk (v0, closed, `is_deleted = true`) → Kharkiv (v7, current);
- customer 100 — Kyiv (v0, closed) → Odesa (v9, current).

`table_changes` for round 1 returned 6 events: a pre/post image pair for each update, one `delete` and one `insert`.

The notebook starts with a reset cell (demo only) so Run all always replays the same scenario. The pipeline function itself never drops or rewrites the target; in production the SCD2 table would also be protected against full reloads, as in Lab 5.

### Discussion

**Batch vs CDC.** A full reload copies the whole table to bring in one change and cannot see deletes — a deleted row simply disappears. CDC moves only changed rows and delivers deletes as events, so the cost grows with the number of changes, not with the table size. The price is state to manage: the pipeline must remember where it stopped (`cdc_checkpoint`).

**Slowly changing dimensions.** In Lab 4 SCD2 was built by comparing CSV snapshots; here it is built from change events. Two things snapshots never had to handle: deletes (closed and flagged, not removed) and a key that comes back after deletion (a new current version with a gap in validity).

**History granularity.** One event per key per batch means the SCD2 table keeps the state at each pipeline run, not at each source commit. Changes between two runs collapse into one: customer 7 went Kyiv → Odesa → Lviv, but the target has no Odesa version and the Kyiv version ends at the Lviv commit. Customer 100 was deleted and re-created, but its Kyiv version is closed as a normal change (`is_deleted = false`), because the delete was dropped by deduplication. Exact history would need processing each source version separately or a sequencing framework (`AUTO CDC` with `sequence_by`, Lab 5).

**Late-arriving data.** Events are ordered by `_commit_version` and `valid_from` / `valid_to` use the commit timestamp — the time the change was written to `customers_src`, not the business time `updated_at`. This keeps versions contiguous and in the order the source committed them, and does not depend on the application setting `updated_at` correctly. The trade-off: if a change arrives late with an older `updated_at`, it still becomes the current version, because commit order wins over business time. When business time matters, the pipeline must sequence by a business column instead — the same role `sequence_by` played in Lab 5, or the LSN in SQL Server CDC.

**Known limitation.** `MERGE` and the checkpoint update are two separate transactions. If the pipeline fails between them, the next run re-reads the same versions: updates would close the already-new version and insert duplicates, so idempotency holds only on the happy path. `_source_version` makes such duplicates detectable. In production Structured Streaming with `readChangeFeed` and a `checkpointLocation`, or `AUTO CDC`, record progress together with the write.