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

`CREATE CONNECTION` (type `postgresql`, password via `secret('lab10', 'neon-password')`) + `CREATE FOREIGN CATALOG` mount the Neon database `neondb` into Unity Catalog as `natalia_neon_catalog`. Only metadata is stored; rows are read from Neon at query time. `DESCRIBE CONNECTION` shows host, port, owner and `Read-only: true` — the password is never displayed.

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

**Security.** The password lives in a secret scope and inside the connection object, never in code or Git. The connection is read-only, so Databricks users cannot change operational data. Access is governed by Unity Catalog grants on the connection and catalog. Weak spot of this setup: Neon had no IP restrictions; in production the source would allow only Databricks egress IPs or a private network.

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

1. reads `last_processed_version` from `cdc_checkpoint` and the latest source version;
2. reads only the new versions with `table_changes()` (empty batch if there are none);
3. drops `update_preimage` and keeps one event per customer — the latest `_commit_version` (`MERGE` allows at most one source row per target row);
4. `MERGE INTO`: update events are fed twice (`merge_key = customer_id` closes the current version, `merge_key = NULL` inserts the new one); a delete closes the current version and sets `is_deleted = true` (soft delete, history kept);
5. moves the checkpoint.

**Scenario:**

| Round | Source changes | Result in the target |
|---|---|---|
| 1 | 7 moves, 15 changes segment, 42 deleted, 1001 inserted | 1002 versions, 1000 current, checkpoint 4 |
| 2 | 7 moves twice, 42 comes back | 1004 versions, 1001 current = 1001 in source, checkpoint 7 |
| 3 | no changes | empty batch, MERGE changed 0 rows, checkpoint stays 7 |

After round 2 the history is: customer 7 — Lutsk (v0, closed) → Kyiv (v1, closed) → Lviv (v6, current); customer 42 — Lutsk (v0, closed, `is_deleted = true`) → Kharkiv (v7, current). `table_changes` for round 1 returned 6 events: a pre/post image pair for each update, one `delete` and one `insert`.

The notebook starts with a reset cell (demo only) so Run all always replays the same scenario. The pipeline function