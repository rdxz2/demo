# Installation

Download Metabase binary file

```sh
wget https://downloads.metabase.com/v0.52.2/metabase.jar
```

Install Java

```sh
sudo apt update -y && sudo apt install -y default-jre
```

# Database setup

## Metabase metadata database

```sql
CREATE USER metabase WITH PASSWORD '12321' LOGIN;
CREATE USER metabase_readonly WITH PASSWORD '12321' LOGIN;

CREATE DATABASE metabase OWNER metabase;

GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase;

\c metabase
-- Publication
CREATE PUBLICATION "metabase" FOR ALL TABLES;
SELECT PG_CREATE_REPLICATION_SLOTS('metabase', 'pgoutput');
GRANT USAGE ON SCHEMA public TO repl;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO repl;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO repl;
-- The owner
GRANT USAGE ON SCHEMA public TO metabase;
GRANT ALL ON SCHEMA public TO metabase;
-- Readonly
GRANT USAGE ON SCHEMA public TO metabase_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO metabase_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO metabase_readonly;
-- Create trigger to alter table replica identity to full for tables that does not have primary key
CREATE OR REPLACE FUNCTION f__set_replica_identity_full()
RETURNS event_trigger AS $$
DECLARE
    obj record;
    has_pk boolean;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF obj.object_type = 'table' THEN
            -- Check if the table has a primary key
            SELECT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = format('%I.%I', obj.schema_name, obj.objid::regclass)::regclass
                  AND contype = 'p'
            ) INTO has_pk;

            -- Apply REPLICA IDENTITY FULL only if no primary key exists
            IF NOT has_pk THEN
                EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL', obj.schema_name, obj.objid::regclass);
            END IF;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
DROP EVENT TRIGGER IF EXISTS tg__create_table__set_replica_identity_full;
CREATE EVENT TRIGGER tg__create_table__set_replica_identity_full
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE')
EXECUTE FUNCTION f__set_replica_identity_full();
```

## Optional SQL

See [optional.sql](./optional.sql) for optional queries than can be run during setup/

# Run Metabase locally

```sh
cd metabase
./run.sh
```
