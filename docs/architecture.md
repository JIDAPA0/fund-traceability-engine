# Architecture

## Overview

The engine follows an ELT flow:
1. Extract source fund and holding data from Thai/global systems.
2. Normalize IDs, tickers, currencies, and fund relationships.
3. Write normalized entities into staging schema.
4. Build trace paths and compute effective exposure.
5. Write consumer-friendly exposure tables into mart schema.

## Database Roles

- `localhost:3306` (`global_funds`): `global_funds_raw`, `global_funds_staging`, `global_funds_mart`
- `localhost:3307` (`fund_traceability`): `fund_traceability_staging`, `fund_traceability_mart`

## Runtime Jobs

- `pipelines/run_build_staging.py`
- `pipelines/run_build_mart.py`
- `pipelines/run_refresh_all.py`

## Orchestration

- Prefect flow entrypoint: `pipelines/prefect/run_refresh_flow.py`
- Flow sequence: smoke test -> staging build -> mart build

## Contracts

- Raw input schema contract: `docs/data_contract.md`
