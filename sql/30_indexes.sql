USE fund_traceability_staging;

CREATE INDEX idx_stg_holdings_fund_id ON stg_holdings (fund_id);
CREATE INDEX idx_stg_holdings_asset_id ON stg_holdings (asset_id);
CREATE INDEX idx_stg_links_feeder ON stg_fund_links (feeder_fund_id);
CREATE INDEX idx_stg_links_master ON stg_fund_links (master_fund_id);

USE fund_traceability_mart;

CREATE INDEX idx_mart_true_exposure_asset ON mart_true_exposure (final_asset_id);
