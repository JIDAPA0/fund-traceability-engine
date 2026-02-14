# fund-traceability-engine

Engine for combining Thai and global fund datasets and calculating true exposure across multi-layer feeder/master structures.

## Quick Start

1. Copy `.env.example` to `.env` and update credentials.
2. Install dependencies:
   - `pip install -e .`
3. Build staging:
   - `python pipelines/run_build_staging.py`
4. Build mart:
   - `python pipelines/run_build_mart.py`

## Project Layout

- `src/` core modules (extract/transform/load/services/utils)
- `pipelines/` executable jobs
- `sql/` database DDL and indexes
- `tests/` unit/integration tests
- `docs/` architecture and data model docs
