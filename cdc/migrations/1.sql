CREATE UNLOGGED TABLE cdc.merge_table (
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    id SERIAL NOT NULL PRIMARY KEY,
    pg_table_fqn VARCHAR(191) NOT NULL,  -- DB name (63) + . (1) + schema name (63) + . (1) + table name (63)
    bq_partition_col VARCHAR(63),
    bq_cluster_cols VARCHAR(63)[],
    bq_validate_cols VARCHAR(63)[] NOT NULL,  -- Validate data using this columns
    latest_cutoff_ts TIMESTAMPTZ NOT NULL DEFAULT TO_TIMESTAMP(0)
);
CREATE INDEX uix_ukm92F4g ON cdc.merge_table(pg_table_fqn);

CREATE UNLOGGED TABLE cdc.merge_history (
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    id BIGSERIAL NOT NULL PRIMARY KEY,
    merge_table_id INT REFERENCES cdc.merge_table(id),
    latest_cutoff_ts TIMESTAMPTZ NOT NULL  -- Must sync with cdc.merge_table.latest_cutoff_ts
);

CREATE INDEX ix_8UJxn8QN ON cdc.merge_history(latest_cutoff_ts);
