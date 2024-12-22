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
CREATE USER repl_readonly WITH PASSWORD '12321' LOGIN;
```

```sh
docker restart pg
```

```sql
CREATE DATABASE stream;
\c stream
CREATE PUBLICATION "stream" FOR ALL TABLES;
GRANT USAGE ON SCHEMA public TO repl_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public to repl_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO repl_readonly;

-- Replication slot will be automatically created by the streamer
```

## CDC metadata database

```sql
CREATE DATABASE cdc OWNER repl;
```

Run this script and execute the output sql to the `cdc` database

```sh
python generate_merger_table.py
```

# Running streamer and uploader the service locally

```sh
docker compose up --build
```

# Deploying

## Building docker image

```sh
# Streamer
docker build -t xz2-demo-cdc-streamer:v0.0.1 --build-arg SSH_PRIVATE_KEY="$(cat __KEY_FILENAME__)" -f Dockerfile.streamer .

# Uploader
docker build -t xz2-demo-cdc-uploader:v0.0.1 --build-arg SSH_PRIVATE_KEY="$(cat __KEY_FILENAME__)" -f Dockerfile.uploader .

# [SIMULATION ONLY] Producer
docker build -t xz2-demo-cdc-producer:v0.0.1 --build-arg SSH_PRIVATE_KEY="$(cat __KEY_FILENAME__)" -f Dockerfile.producer .
```

## Running docker image

```sh
docker run -d --name __NAME__ -v __DOTENV_FILENAME__:/app/.env -v __LOGS_DIR__:/app/logs -v __OUTPUT_DIR__:/app/output -v __SA_FILENAME__:/app/sa.json --network host xz2-demo-cdc-streamer:v0.0.1
docker run -d --name __NAME__ -v __DOTENV_FILENAME__:/app/.env -v __LOGS_DIR__:/app/logs -v __OUTPUT_DIR__:/app/output -v __SA_FILENAME__:/app/sa.json --network host xz2-demo-cdc-uploader:v0.0.1
docker run -d --name __NAME__ -v __DOTENV_FILENAME__:/app/.env --network host xz2-demo-cdc-producer:v0.0.1
```
