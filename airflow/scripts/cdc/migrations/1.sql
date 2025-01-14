CREATE TABLE public.merger (
    "create_ts" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "update_ts" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "is_active" BOOLEAN NOT NULL DEFAULT TRUE,
    "id" SERIAL PRIMARY KEY,
    "database" VARCHAR(63) NOT NULL,
    "schema" VARCHAR(63) NOT NULL,
    "table" VARCHAR(63) NOT NULL,
    "partition_col" VARCHAR(63),
    "cluster_cols" VARCHAR(63)[],
    "validate_cols" VARCHAR(63)[] NOT NULL,  -- Validate data using this columns
    "last_cutoff_ts" TIMESTAMPTZ DEFAULT TO_TIMESTAMP(0)
);

CREATE UNIQUE INDEX uidx_xsbpbneq ON public.merger ("database", "schema", "table");
