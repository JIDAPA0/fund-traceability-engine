# Sample Data

Purpose:

- bootstrap local test inputs before real raw data arrives
- cover edge cases in normalization + traceability logic

Files:

- `raw_funds_sample.csv`: base master fund list
- `raw_holdings_sample_with_edge_cases.csv`: multilayer graph, cycle, missing/invalid rows
- `raw_fund_links_sample_with_edge_cases.csv`: feeder->master links with confidence + invalid row
- `expected_true_exposure_sample.csv`: expected exposure snapshot for manual sanity checks
