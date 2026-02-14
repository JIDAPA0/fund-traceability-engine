USE fund_traceability_staging;

CREATE TABLE IF NOT EXISTS stg_funds (
  fund_id VARCHAR(128) PRIMARY KEY,
  fund_name VARCHAR(512) NOT NULL,
  source VARCHAR(32) NOT NULL,
  currency VARCHAR(8),
  as_of_date DATE,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_holdings (
  fund_id VARCHAR(128) NOT NULL,
  asset_id VARCHAR(128) NOT NULL,
  asset_name VARCHAR(512),
  asset_type VARCHAR(32),
  weight DECIMAL(18,8),
  as_of_date DATE,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_fund_links (
  feeder_fund_id VARCHAR(128) NOT NULL,
  master_fund_id VARCHAR(128) NOT NULL,
  confidence DECIMAL(8,6),
  as_of_date DATE,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
