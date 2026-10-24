# Lab 8 — CI/CD: DEV → PROD promotion

## Overview

The whole YouTube project (Lakeflow pipeline, gold notebooks, schemas, dashboard and permissions) is described as one Databricks Asset Bundle and promoted to PROD automatically by GitHub Actions.

Nothing in PROD is created or changed by hand. A pull request runs tests and validation; a merge to `main` deploys the bundle to PROD and runs the jobs.

## Environments

The same bundle is deployed to two workspaces; only the target settings differ.

|  | DEV | PROD |
| --- | --- | --- |
| Workspace | `dbr_dev` (`adb-7405604651938730.10`) | `dbr_dev_ua_5816_trail_3` (`adb-7405613488297663.3`) |
| Catalog | `dbr_dev_ua5816bd` | `dbr_dev_ua_5816_trail_3` |
| Schemas | `natalkamartinuk55_bronze` / `_silver` / `_gold` | `natalkamartinuk55_youtube_bronze` / `_silver` / `_gold` |
| Bundle mode | `development` (`[dev natalkamartinuk55]` prefix, paused schedules) | `production` |
| Who deploys | developer, from a laptop | GitHub Actions only |
| Runs as | developer | service principal `sp-databricks-adls` |

Both workspaces use the same Unity Catalog metastore, so PROD can read the original source files from the DEV volume.

## Bundle

One bundle, `youtube_lakeflow`, describes the whole project. Its name is the same as in Lab 5 and Lab 7, so in DEV the deployment updates the existing pipeline instead of creating a second one that would write to the same tables.

Everything that differs between environments is a bundle variable set in the target: `catalog`, `bronze_schema`, `silver_schema`, `gold_schema`, `raw_volume_path`. The SQL warehouse is found by name (`lookup`), so each workspace gets its own warehouse ID.

The values reach the code in two ways:

- the pipeline gets them through `${var...}` in `pipeline.yml`;
- the gold notebooks get them as job parameters; `00_config` reads them from widgets and adds them to the shared settings from `gold_config.yml`.

The catalog name is written in one place only, the target. `gold_config.yml` keeps only settings that are the same everywhere: source table names, business period, list of gold tables.

### Everything as code

| Object | How it is defined |
| --- | --- |
| Pipeline | `resources/youtube_lakeflow_etl.pipeline.yml` |
| Jobs | `resources/jobs.yml` |
| Schemas and raw volume (PROD) | `resources.schemas` and `resources.volumes` in the `prod` target |
| Gold tables and constraints | `01_gold_ddl` and `04_gold_constraints`, run by the `gold_setup` job |
| Notebooks | files in `notebooks/`, uploaded by the bundle |
| Dashboard and its queries | `dashboards/youtube_trending.lvdash.json` + `resources/dashboard.yml` |
| Permissions | `permissions` (jobs, pipeline, dashboard) and `grants` (schemas) in the `prod` target |
| Event Hub secret (PROD) | created by the workflow from a GitHub secret |

### Jobs

**`gold_setup`** — structure only: `gold_ddl` → `gold_constraints`. No schedule; it runs after every deployment. DDL is kept out of the data job, so structure changes happen at deploy time and the data job only loads data.

**`youtube_etl`** — data only: `copy_raw_files` → `refresh_pipeline` → `gold_dimensions` → `gold_facts` → `gold_aggregates`. `copy_raw_files` copies the top-level CSV files from the DEV volume into the volume of the current target. In DEV the source and the target are the same volume, so the task does nothing.

**`unit_tests`** — runs the 13 pytest tests. Used by CI on pull requests.

All jobs run on serverless compute and have `max_concurrent_runs: 1` and timeouts. The data jobs also have one retry and an email on failure.

The DQ suite (`05_dq_suite`) is not part of CI/CD: it checks data, not code, and runs as a separate process.

## CI/CD

Work is done in the `dev` branch; `main` is what is deployed to PROD. Changes reach `main` only through pull requests.

The workflow runs only when files in `lab8/` or the workflow file change. The deploy job runs only on `main` and never in parallel with another deploy (`concurrency`).

### Authentication

| Where | Identity | GitHub secrets |
| --- | --- | --- |
| DEV | developer personal access token | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` |
| PROD | service principal `sp-databricks-adls` (Microsoft Entra ID) | `PROD_DATABRICKS_HOST`, `PROD_ARM_CLIENT_ID`, `PROD_ARM_TENANT_ID`, `PROD_ARM_CLIENT_SECRET` |
| Event Hub | connection string | `EVENTHUB_CONNECTION_STRING` |

The service principal is added to the PROD workspace and has `USE CATALOG` and `CREATE SCHEMA` on the PROD catalog, plus read access to the source volume in DEV. It creates the PROD schemas, so it owns them; the developer gets read-only access through `grants`.

### Event Hub secret

The pipeline reads the Event Hub connection string from the secret scope `natalkamartinuk55_scope`. A secret scope exists only in the workspace where it was created, so before every PROD deployment the workflow creates the scope (if it does not exist) and writes the secret from GitHub Secrets.

This step runs outside the bundle folder: inside it, the CLI would use the default `dev` target instead of `DATABRICKS_HOST`.

## Idempotency

A deployment can be repeated any number of times with the same result:

- the bundle keeps a deployment state, so a repeated deploy updates existing resources instead of creating new ones;
- DDL uses `CREATE TABLE IF NOT EXISTS`; constraints are dropped (if they exist) and added again;
- gold tables are loaded with `MERGE`, not `INSERT`;
- Auto Loader does not read the same file twice;
- `copy_raw_files` copies only files that are not in the target yet;
- the secret scope is created only if it is missing.

**Check.** Row counts of the bronze, silver and gold tables in PROD were recorded, then a second pull request was merged and deployed. After the second deployment the counts were the same, `copy_raw_files` reported `Copied 0 of 1 files`, and the bundle did not create any resource again.

## Rollback

`main` always matches PROD. To roll back, revert the faulty commit in `dev`, open a pull request and merge it: the workflow deploys the previous version of the bundle. No manual changes are made in PROD.

## Decisions

- **Separate PROD workspace.** A clean workspace proves that the project does not depend on anything created by hand (clusters, schemas, volumes), and it allows restricting who can change PROD.
- **Own schema names in PROD.** The PROD catalog is shared, so schemas use the `natalkamartinuk55_youtube_` prefix.
- **Test data is not promoted.** Only top-level CSV files are copied; the Auto Loader schema folder and the simulated batches from Lab 5 stay in DEV.
