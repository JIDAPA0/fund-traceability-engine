# Data Model

## Staging Tables

- `stg_funds`: canonical fund identity and metadata.
- `stg_holdings`: direct holdings by fund.
- `stg_fund_links`: feeder-to-master relationship candidates.

## Mart Tables

- `mart_true_exposure`: final rolled-up exposure by root fund and terminal asset.

## Key Design Notes

- `fund_id` and `asset_id` should be stable canonical IDs.
- `as_of_date` enables point-in-time rebuilds.
- `loaded_at` / `calculated_at` support operational observability.
