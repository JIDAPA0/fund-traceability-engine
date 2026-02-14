USE fund_traceability_mart;

CREATE TABLE IF NOT EXISTS mart_true_exposure (
  root_fund_id VARCHAR(128) NOT NULL,
  final_asset_id VARCHAR(128) NOT NULL,
  effective_weight DECIMAL(18,8) NOT NULL,
  path_depth INT NOT NULL,
  as_of_date DATE,
  calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (root_fund_id, final_asset_id, as_of_date)
);
