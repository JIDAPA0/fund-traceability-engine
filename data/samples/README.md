# Sample Data

Purpose:

- bootstrap local test inputs before real raw data arrives
- cover edge cases in normalization + traceability logic

Files:

- `raw_funds_sample.csv`: base master fund list
- `raw_holdings_sample_with_edge_cases.csv`: multilayer graph, cycle, missing/invalid rows
- `raw_fund_links_sample_with_edge_cases.csv`: feeder->master links with confidence + invalid row
- `expected_true_exposure_sample.csv`: expected exposure snapshot for manual sanity checks

Demo Flow:

1. Load sample CSVs into `global_funds_raw`:
   - `python3 pipelines/run_load_samples_to_raw.py --as-of-date 2026-02-14`
2. Build staging:
   - `python3 pipelines/run_build_staging.py --as-of-date 2026-02-14`
3. Build mart:
   - `python3 pipelines/run_build_mart.py --as-of-date 2026-02-14 --max-depth 6`
4. Validate actual mart output against expected sample:
   - `python3 pipelines/run_validate_sample_expectation.py --as-of-date 2026-02-14`
