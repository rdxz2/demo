# PostgreSQL installation

## CDC

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

### Streamed database

```sql
ALTER SYSTEM SET wal_level = logical;
SHOW wal_level;

CREATE USER repl WITH PASSWORD '12321';
ALTER ROLE repl WITH REPLICATION LOGIN;
```

```sh
docker restart pg
```

```sql
CREATE DATABASE stream;
GRANT SELECT ON ALL TABLES IN SCHEMA public to repl;
\c stream
CREATE PUBLICATION "stream" FOR ALL TABLES;

-- Replication slot will be automatically created by the streamer
```
