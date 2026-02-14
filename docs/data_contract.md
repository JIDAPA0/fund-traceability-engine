# Data Contract (Raw Layer)

This contract defines the minimum schema expected by `pipelines/run_build_staging.py` for ingesting global raw data and producing traceability staging tables.

## Scope

- Source server: `localhost:3306`
- Canonical raw tables:
  - `raw_funds`
  - `raw_holdings`
  - `raw_fund_links`
- Alternate table names accepted by current loader:
  - Funds: `funds`, `global_funds`, `master_funds`, `fund_master`
  - Holdings: `holdings`, `global_holdings`, `fund_holdings`
  - Links: `fund_links`, `feeder_master_links`, `feeder_master_map`

## Table: raw_funds

Required columns:

- `fund_id` (`VARCHAR(128)`): stable unique fund key, non-empty

Recommended columns:

- `fund_name` (`VARCHAR(512)`): human-readable name
- `source` (`VARCHAR(64)`): source/provider label (e.g., `global`)
- `currency` (`VARCHAR(16)`): asset base currency (e.g., `USD`, `THB`)

Accepted aliases used by loader:

- `fund_id`: `id`, `master_fund_id`, `isin`, `fund_code`, `ticker`
- `fund_name`: `name`, `master_fund_name`, `fund_title`
- `source`: `source_system`, `provider`
- `currency`: `currency_code`, `ccy`

Quality rules:

- `fund_id` must be non-null, trimmed, unique within snapshot.
- Duplicate `fund_id` rows are de-duplicated by keeping first occurrence in current loader.

## Table: raw_holdings

Required columns:

- `fund_id` (`VARCHAR(128)`): parent/master fund ID, non-empty
- `asset_id` (`VARCHAR(128)`): child asset/fund ID, non-empty
- `weight` (`DECIMAL(18,8)` or numeric text): holding weight

Recommended columns:

- `asset_name` (`VARCHAR(512)`)
- `asset_type` (`VARCHAR(32)`), e.g. `fund`, `equity`, `bond`, `etf`, `cash`

Accepted aliases used by loader:

- `fund_id`: `master_fund_id`, `parent_fund_id`, `portfolio_id`
- `asset_id`: `holding_id`, `ticker`, `symbol`, `security_id`
- `weight`: `holding_weight`, `allocation`, `pct`, `percentage`
- `asset_name`: `holding_name`, `security_name`, `name`
- `asset_type`: `holding_type`, `security_type`, `type`

Quality rules:

- `weight` can be in `0..1` or `0..100`; values > 1 are interpreted as percent and divided by 100.
- Negative weights are clipped to `0`, values above 1 are clipped to `1` after normalization.
- Rows with missing `fund_id` or `asset_id` are dropped by loader.

## Table: raw_fund_links

Required columns:

- `feeder_fund_id` (`VARCHAR(128)`): Thai feeder fund ID, non-empty
- `master_fund_id` (`VARCHAR(128)`): mapped global master fund ID, non-empty

Recommended columns:

- `confidence` (`DECIMAL(8,6)`): link confidence in range `0..1`

Accepted aliases used by loader:

- `feeder_fund_id`: `feeder_id`, `thai_fund_id`, `fund_id`
- `master_fund_id`: `master_id`, `target_fund_id`, `linked_fund_id`
- `confidence`: `score`, `match_score`

Quality rules:

- `confidence` defaults to `1.0` when missing/non-numeric.
- `confidence` is clipped to range `0..1`.
- Rows with missing feeder/master IDs are dropped by loader.

## Snapshot / Partition Rules

- Pipeline runtime parameter `--as-of-date` controls target partition date in staging/mart tables.
- Re-runs for the same `as_of_date` are idempotent: existing partition rows are deleted before insert.

## Consumer Tables Produced

- `fund_traceability_staging.stg_funds`
- `fund_traceability_staging.stg_holdings`
- `fund_traceability_staging.stg_fund_links`
