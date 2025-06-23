# Hello

My name is Richard and I am a data engineer working with ELT pipelines and infrastructure on daily-basis.

This demo project will showcase my various capabilities as data engineer:

- Daily public data ingestion with Airflow into BigQuery
- CDC, deliver PostgreSQL transactions (WAL logs) directly into BigQuery
- Daily BigQuery data backup using GCS
- Insights extraction via dashboard, using Metabase
- Wrap them up in a single Streamlit site
- Alerting into Discord webhook

All the service above (except the fully-managed services like BigQuery and GCS) are self-hosted in GCP, with flow diagram below

_[placeholder]_

This readme file will explain all the components needed to run the project.

# Appendix 1. development guide

To begin develop

## Setup PostgreSQL

Create container

```sh
docker volume create demo-pg

docker run \
    --name demo-pg \
    -d \
    -p 40000:5432 \
    -e POSTGRES_PASSWORD=12321 \
    -v demo-pg:/var/lib/postgresql/data \
    postgres:17

docker exec -it demo-pg psql -U postgres
```

Change WAL level into logical

```sql
alter system set wal_level = logical;
show wal_level;  -- Should print logical
```

Restart container so previous configurations is loaded

```sh
docker restart demo-pg

docker exec -it demo-pg psql -U postgres
```

Set up "dummy" application database with replication

```sql
create user dummy with password '12321' login;
create database dummydb owner dummy;

create user repl with password '12321' login replication;

\c dummydb
create publication "repl" for all tables;
select pg_create_replication_slots('repl', 'pgoutput');
```

## Handful scripts

```sh
# Build CDC services
cd cdc
docker build -t xz2-demo-cdc-streamer:v0.0.1 --secret id=ssh_key,src=/home/ubuntu/.ssh/access-key-bitbucket -f Dockerfile.streamer .
docker build -t xz2-demo-cdc-uploader:v0.0.1 --secret id=ssh_key,src=/home/ubuntu/.ssh/access-key-bitbucket -f Dockerfile.uploader .
docker build -t xz2-demo-cdc-producer:v0.0.1 --secret id=ssh_key,src=/home/ubuntu/.ssh/access-key-bitbucket -f Dockerfile.producer .

# Run CDC services (stream database)
cd cdc
docker rm -f cdc-streamer-stream && docker run --name cdc-streamer-stream -v ./.env.stream:/app/.env -v ./dockerlogs:/app/logs -v ./output:/app/output -v ./sa.json:/app/sa.json --network host xz2-demo-cdc-streamer:v0.0.1
docker rm -f cdc-uploader-stream && docker run --name cdc-uploader-stream -v ./.env.stream:/app/.env -v ./dockerlogs:/app/logs -v ./output:/app/output -v ./sa.json:/app/sa.json --network host xz2-demo-cdc-uploader:v0.0.1
docker rm -f cdc-producer-stream && docker run --name cdc-producer-stream -v ./.env.stream:/app/.env --network host xz2-demo-cdc-producer:v0.0.1

# Run CDC services (metabase database)
cd cdc
docker rm -f cdc-streamer-metabase && docker run --name cdc-streamer-metabase -v ./.env.metabase:/app/.env -v ./dockerlogs:/app/logs -v ./output:/app/output -v ./sa.json:/app/sa.json --network host xz2-demo-cdc-streamer:v0.0.1
docker rm -f cdc-uploader-metabase && docker run --name cdc-uploader-metabase -v ./.env.metabase:/app/.env -v ./dockerlogs:/app/logs -v ./output:/app/output -v ./sa.json:/app/sa.json --network host xz2-demo-cdc-uploader:v0.0.1

# Stop all CDC services
docker ps --filter name=cdc-* -q | xargs docker stop

# Start CDC service (previously created)
docker start -a cdc-streamer-stream
docker start -a cdc-uploader-stream
docker start -a cdc-producer-stream
docker start -a cdc-streamer-metabase
docker start -a cdc-uploader-metabase

# Run metabase
cd metabase
./run.sh

# Run airflow webserver
airflow webserver

# Run airflow scheduler
airflow scheduler

```

# Deployment
