# PostgreSQL Installation

```sh
docker volume create pg__data

docker run \
    --name pg \
    -d \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=12321 \
    -v pg__data:/var/lib/postgresql/data \
    postgres:16
```

# Set up

## Replication

```sql
CREATE USER repl WITH PASSWORD '12321';
CREATE DATABASE stream;

ALTER ROLE repl WITH REPLICATION LOGIN;
ALTER SYSTEM SET wal_level = logical;
```

```sh
docker restart pg
```

```sql
\c stream

CREATE PUBLICATION publ_stream FOR ALL TABLES;

-- Replication slot will be automatically created by the streamer
```
