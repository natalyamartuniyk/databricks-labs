# Lab 9 — Databricks REST API Automation

## Overview

A Python CLI manages Databricks without the UI: it creates a temporary cluster, runs a notebook on it, triggers the ETL job and the Lakeflow pipeline, waits for them and reports the result. It uses the Databricks Python SDK, which sends requests to the REST API (e.g. `jobs.run_now` → `POST /api/2.1/jobs/run-now`). The same script runs in GitHub Actions on every pull request.

## Layout

```
lab9/
├── cli.py            entry point: commands, logging, exit code
├── config.yml        environment values (cluster, notebook, job and pipeline names)
├── requirements.txt
└── automation/
    ├── client.py     workspace connection
    ├── clusters.py   create, wait, delete a temporary cluster
    ├── jobs.py       notebook run, job run, waiting, report
    └── pipelines.py  pipeline update with its own polling loop
```

The notebooks, job and pipeline are deployed by the Lab 8 bundle; the script finds them by path or name.

## Commands

```bash
python cli.py [--target dev] <command>
```

| Command | What it does |
| --- | --- |
| `run-pipeline` | starts an incremental pipeline update and waits |
| `run-job` | triggers `youtube_etl` and waits |
| `run-notebook` | temporary cluster → `05_dq_suite` → cluster deleted |
| `e2e` | `run-job`, then `run-notebook` (skipped if the ETL fails) |
| `status <run_id>` | state of any run |
| `cleanup` | deletes leftover temporary clusters |

Exit code `0` on success, `1` on failure.

## How it works

- Credentials are not in the code: the SDK reads the `academy` profile locally and `DATABRICKS_HOST` / `DATABRICKS_TOKEN` in CI.
- Operations are asynchronous: the script polls the state until a terminal state or a timeout. A run is successful only with `result_state = SUCCESS`.
- The temporary cluster (single node, `SINGLE_USER` for Unity Catalog, policy `Personal Compute`) is deleted in `finally`.
- On failure the script logs task errors (`get_run_output`) or pipeline `ERROR` events.

## CI/CD

The Lab 8 workflow now also runs on `lab9/**`. In the DEV job, after the unit tests: set up Python → install requirements → `python cli.py e2e` → `python cli.py cleanup` (`if: always()`). A pull request is green only if the ETL job and the DQ suite pass in DEV.

## Decisions

- **Temporary cluster** instead of GP1: exists only while needed, same configuration every time.
- **`runs/submit`** for the notebook: no job object is created, the bundle stays the only source of jobs.
- **DEV only**: the check runs before merge; PROD would need a service principal with cluster policy access.
- **DQ suite in CI**: in Lab 8 it ran separately.
