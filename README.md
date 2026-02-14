# fund-traceability-engine

Engine for combining Thai and global fund datasets and calculating true exposure across multi-layer feeder/master structures.

## Quick Start

1. Copy `.env.example` to `.env` and update credentials.
2. Install dependencies:
   - `pip install -e .`
3. Build staging:
   - `python3 pipelines/run_build_staging.py`
4. Build mart:
   - `python3 pipelines/run_build_mart.py`
5. Optional DB connectivity smoke test:
   - `python3 pipelines/run_db_smoke_test.py`

## Prefect Orchestration

Install orchestration extras:

- `pip install -e ".[orchestration]"`

Run full orchestration flow (smoke test -> staging -> mart):

- `python3 pipelines/prefect/run_refresh_flow.py --as-of-date 2026-02-14 --max-depth 6`

Skip DB smoke test in flow:

- `python3 pipelines/prefect/run_refresh_flow.py --skip-smoke-test`

## Docker (Prepared)

Build image:

- `docker build -t fund-traceability-engine:local .`

Run standard pipeline job:

- `docker compose --profile job up --build engine-job`

Run Prefect flow job:

- `docker compose --profile prefect up --build prefect-flow`

## Database Topology

- `3306` (`global_funds` server): global-source data stores
- `3307` (`fund_traceability` server): feeder-master linking and exposure outputs

Default database names in `.env.example`:

- `global_funds_raw`
- `global_funds_staging`
- `global_funds_mart`
- `fund_traceability_staging`
- `fund_traceability_mart`

## Project Layout

- `src/` core modules (extract/transform/load/services/utils)
- `pipelines/` executable jobs
- `sql/` database DDL and indexes
- `tests/` unit/integration tests
- `docs/` architecture and data model docs

## Data Contract

- Raw-layer contract is documented at `docs/data_contract.md`.
- Use sample raw inputs in `data/samples/` while real upstream data is not ready.

## Unit Tests

- Run: `python3 -m unittest discover -s tests/unit -p 'test_*.py'`

## Sample Demo (No Upstream Data Needed)

1. Load bundled sample raw CSVs:
   - `python3 pipelines/run_load_samples_to_raw.py --as-of-date 2026-02-14`
2. Build staging:
   - `python3 pipelines/run_build_staging.py --as-of-date 2026-02-14`
3. Build mart:
   - `python3 pipelines/run_build_mart.py --as-of-date 2026-02-14 --max-depth 6`
4. Validate mart output against expected sample:
   - `python3 pipelines/run_validate_sample_expectation.py --as-of-date 2026-02-14`
