# Lab 7 — Data Quality Testing & Unit Tests

Lab 7 adds tests to the YouTube trending pipeline from Labs 5 and 6. Unit tests check the
transformation code, data quality checks check the data in bronze, silver and gold. Both run in
GitHub Actions after every push and fail the workflow when something is broken.

- Workspace: `adb-7405604651938730.10.azuredatabricks.net`, catalog `dbr_dev_ua5816bd`
- Schemas: `natalkamartinuk55_bronze`, `_silver`, `_gold`
- Compute: shared cluster GP1, Databricks Runtime 17.3 LTS

## Layout

```
.github/workflows/lab7-ci.yml      CI workflow
lab7/
├── databricks.yml                 bundle with jobs unit_tests and dq_suite
├── src/youtube_gold/              transformation modules: scope, dimensions, facts
├── tests/                         13 pytest unit tests
├── notebooks/                     gold notebooks, constraints, DQ suite, test data, test runner
└── youtube_lakeflow/              Lakeflow pipeline (moved from Lab 5)
```

Lab 5 and Lab 6 folders are unchanged. The pipeline bundle in `lab7/youtube_lakeflow` has the same
name as in Lab 5, so it updates the same pipeline and tables. It should be deployed only from `lab7`.

## Part A — Unit tests

The transformation logic from the Lab 6 notebooks moved into the `youtube_gold` package. Every
function takes DataFrames and returns a DataFrame; tables it needs are passed as arguments.
Notebooks only read tables, call the functions and write the result. After the refactoring every
gold MERGE reported 0 inserted, 0 updated and 0 deleted rows, so the data did not change.

The 13 tests cover date parsing (`yy.dd.MM`), inclusive period boundaries, calendar generation and
weekends, deduplication, the channel lookup without row loss, `views_delta` and `trending_day_num`
per video, the full silver to fact chain and orphan detection.

The same tests run in three places. `conftest.py` uses the cluster session on Databricks and
Databricks Connect (profile `academy`) on the laptop.

```bash
.venv/bin/pytest -v                    # laptop, Databricks Connect
databricks bundle run unit_tests       # headless job
```

## Part B — Data quality

**Expectations (silver).** Rules are split by reaction: drop rules (nulls, negative counters, date
format) and warn rules (`video_id` length, known category, published before trending). The quarantine
uses the same rules and stores the violated ones in `_dq_failed_rules`. A test batch from
`dq_simulate_bad_batch` sent 3 rows to quarantine with the expected reasons and 3 rows to silver
with warnings.

**Delta constraints.** `04_gold_constraints` adds three CHECK constraints to `dim_date`. An INSERT
with one valid and one invalid row is rejected as a whole.

**DQ suite.** `05_dq_suite` runs 22 checks: 15 on gold with DQX (`dq_checks.yml`), 4 on bronze and
silver with SQL, and 3 reconciliation checks:

| Check | Expected |
|---|---|
| bronze vs silver + quarantine | equal |
| distinct video-days in silver vs fact rows | equal |
| `SUM(views_delta)` in fact vs category aggregate | equal |

Results are appended to `gold.dq_results`. If any error-level check fails, the notebook raises and
the job fails.

## Decisions and findings

**Fail rule.** `trending_date_format` was first a fail expectation. A bad date stopped the pipeline
on every run, because the checkpoint does not move past a failed batch, and bronze is append-only.
The rule became a drop rule, and a format change in the source is caught by a suite check on the
share of bad dates (error above 1%).

**Lost row.** During those failed runs the quarantine processed the test row under the old rules,
and later silver dropped it. Reconciliation found it: bronze had one row more than silver plus
quarantine. The row cannot be recovered, so it is excluded from the check by a commented list.

**Constraints on the fact.** CHECK constraints are not supported on a table with a row filter, and
the fact has one from Lab 6. The fact rules are checked in the DQ suite instead.

**Data.** 35 fact rows have negative `views_delta`, which is why this rule is a warning.
All error-level checks on gold pass.

## CI

`lab7-ci.yml` runs on push to `main` when `lab7/` changes: bundle deploy, unit tests, DQ suite.
It needs the secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`.