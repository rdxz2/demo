# Virtual env installation

```sh
conda create -n xz2democdc312 python=3.12 -y
conda activate xz2democdc312
pip install -r requirements.txt
```

# Database setup

## CDC metadata database

```sql
CREATE DATABASE cdc;

GRANT ALL PRIVILEGES ON DATABASE cdc TO repl;

\c cdc
-- Master
GRANT USAGE ON SCHEMA public TO repl;
GRANT ALL ON SCHEMA public TO repl;
-- Readonly
GRANT USAGE ON SCHEMA public TO repl_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO repl_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO repl_readonly;
```

## Sample streamed database

```sql
ALTER SYSTEM SET wal_level = logical;
SHOW wal_level;
```

```sh
docker restart pg
```

```sql
CREATE USER repl WITH PASSWORD '12321' LOGIN REPLICATION;
CREATE USER repl_readonly WITH PASSWORD '12321' LOGIN;

CREATE DATABASE dummydb;

GRANT ALL PRIVILEGES ON DATABASE dummydb TO repl;

\c dummydb
-- Publication
CREATE PUBLICATION "repl" FOR ALL TABLES;
SELECT PG_CREATE_REPLICATION_SLOTS('repl', 'pgoutput');
GRANT USAGE ON SCHEMA public TO repl_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO repl_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO repl_readonly;
-- The owner
GRANT USAGE ON SCHEMA public TO repl;
GRANT ALL ON SCHEMA public TO repl;
-- Readonly
GRANT USAGE ON SCHEMA public TO repl_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO repl_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO repl_readonly;
```

Run this script after populating the example stream table and execute the output sql to the `cdc` database

```sh
python generate_merger_table.py
```

# Building docker image

```sh
# Streamer
docker build -t xz2-demo-cdc-streamer:__VERSION__ --secret id=ssh_key,src=__ACCESS_KEY_FILENAME__ -f Dockerfile.streamer .

# Uploader
docker build -t xz2-demo-cdc-uploader:__VERSION__ --secret id=ssh_key,src=__ACCESS_KEY_FILENAME__ -f Dockerfile.uploader .

# [SIMULATION ONLY] Producer
docker build -t xz2-demo-cdc-producer:__VERSION__ --secret id=ssh_key,src=__ACCESS_KEY_FILENAME__ -f Dockerfile.producer .
```

# Running docker image

```sh
docker run -d --name __NAME__ -v __DOTENV_FILENAME__:/app/.env -v __LOGS_DIR__:/app/logs -v __OUTPUT_DIR__:/app/output -v __SA_FILENAME__:/app/sa.json --network host xz2-demo-cdc-streamer:__VERSION__
docker run -d --name __NAME__ -v __DOTENV_FILENAME__:/app/.env -v __LOGS_DIR__:/app/logs -v __OUTPUT_DIR__:/app/output -v __SA_FILENAME__:/app/sa.json --network host xz2-demo-cdc-uploader:__VERSION__
docker run -d --name __NAME__ -v __DOTENV_FILENAME__:/app/.env --network host xz2-demo-cdc-producer:__VERSION__
```
