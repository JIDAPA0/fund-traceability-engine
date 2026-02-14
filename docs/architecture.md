# Architecture

## Overview

The engine follows an ELT flow:
1. Extract source fund and holding data from Thai/global systems.
2. Normalize IDs, tickers, currencies, and fund relationships.
3. Write normalized entities into staging schema.
4. Build trace paths and compute effective exposure.
5. Write consumer-friendly exposure tables into mart schema.

## Runtime Jobs

- `pipelines/run_build_staging.py`
- `pipelines/run_build_mart.py`
- `pipelines/run_refresh_all.py`
