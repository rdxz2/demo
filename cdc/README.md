# Virtual env installation

```sh
conda create -n xz2democdc312 python=3.12 -y
conda activate xz2democdc312
pip install -r requirements.txt
```

# Database setup

## Sample streamed database

```sql
ALTER SYSTEM SET wal_level = logical;
SHOW wal_level;

CREATE USER repl WITH PASSWORD '12321' LOGIN REPLICATION;
```

```sh
docker restart pg
```

```sql
CREATE DATABASE stream;
GRANT SELECT ON ALL TABLES IN SCHEMA public to repl;
\c stream
CREATE PUBLICATION "stream" FOR ALL TABLES;
GRANT USAGE ON SCHEMA public TO repl;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO repl;

-- Replication slot will be automatically created by the streamer
```

## CDC metadata database

```sql
CREATE DATABASE cdc OWNER repl;
```

Add sample streamed database tables to metadata

```sql
INSERT INTO merger ("database", "schema", "table", "partition_col", "cluster_cols", "validate_cols")
VALUES
('stream', 'public', 'all_dtype', 't_ts', ARRAY['t_smallint', 't_int'], ARRAY['t_int', 't_bigint', 't_varchar', 't_text', 't_json', 't_double', 't_bool', 't_ts', 't_dt', 't_date', 't_time']),
('stream', 'public', 'gen_0', NULL, NULL, ARRAY['value1']),
('stream', 'public', 'gen_1', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_2', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_3', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_4', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_5', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_6', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_7', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_8', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_9', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_10', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_11', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_12', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_13', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_14', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_15', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_16', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_17', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_18', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_19', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_20', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_21', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_22', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_23', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_24', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_25', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_26', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_27', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_28', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_29', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_30', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_31', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_32', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_33', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_34', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_35', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_36', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_37', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_38', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_39', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_40', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_41', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_42', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_43', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_44', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_45', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_46', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_47', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_48', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_49', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_50', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_51', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_52', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_53', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_54', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_55', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_56', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_57', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_58', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_59', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_60', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_61', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_62', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_63', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_64', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_65', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_66', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_67', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_68', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_69', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_70', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_71', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_72', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_73', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_74', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_75', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_76', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_77', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_78', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_79', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_80', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_81', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_82', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_83', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_84', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_85', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_86', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_87', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_88', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_89', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_90', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_91', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_92', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_93', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_94', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_95', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_96', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_97', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_98', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'gen_99', 'value3', ARRAY['value1'], ARRAY['value1', 'value2', 'value3']),
('stream', 'public', 'ttruncate', NULL, ARRAY['value1'], ARRAY['value1']),
('stream', 'public', 'tupdate', NULL, ARRAY['value1'], ARRAY['value1']),
('stream', 'public', 'tdelete', NULL, ARRAY['value1'], ARRAY['value1']);
```

# Running streamer and uploader the service locally

```sh
docker compose up --build
```

# Deploying

## Building docker image

```sh
docker build -t xz2-demo-cdc-streamer:v0.0.1 Dockerfile.streamer
docker build -t xz2-demo-cdc-uploader:v0.0.1 Dockerfile.uploader
```

# Running merger script

```sh
conda activate xz2democdc312
python cdc_merger.py
```
