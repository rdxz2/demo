# PostgreSQL installation

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

# Handful scripts

```sh
# Run CDC services (stream database)
cd cdc
docker run -d --name cdc-streamer-stream -v ./.env.stream:/app/.env -v ./dockerlogs:/app/logs -v ./output:/app/output -v ./sa.json:/app/sa.json --network host xz2-demo-cdc-streamer:v0.0.1 && docker logs -f cdc-streamer-stream
docker run -d --name cdc-uploader-stream -v ./.env.stream:/app/.env -v ./dockerlogs:/app/logs -v ./output:/app/output -v ./sa.json:/app/sa.json --network host xz2-demo-cdc-uploader:v0.0.1 && docker logs -f cdc-uploader-stream
docker run -d --name cdc-producer-stream -v ./.env.stream:/app/.env --network host xz2-demo-cdc-producer:v0.0.1 && docker logs -f cdc-producer-stream

# Run CDC services (metabase database)
cd cdc
docker run -d --name cdc-streamer-metabase -v ./.env.metabase:/app/.env -v ./dockerlogs:/app/logs -v ./output:/app/output -v ./sa.json:/app/sa.json --network host xz2-demo-cdc-streamer:v0.0.1 && docker logs -f cdc-streamer-metabase
docker run -d --name cdc-uploader-metabase -v ./.env.metabase:/app/.env -v ./dockerlogs:/app/logs -v ./output:/app/output -v ./sa.json:/app/sa.json --network host xz2-demo-cdc-uploader:v0.0.1 && docker logs -f cdc-uploader-metabase

# Stop CDC services
docker ps --filter name=cdc-* -q | xargs docker stop

# Start CDC service (created previously)
docker start cdc-streamer-stream
docker start cdc-uploader-stream
docker start cdc-producer-stream
docker start cdc-streamer-metabase
docker start cdc-uploader-metabase

# Run metabase
cd metabase
./run.sh

# Run prefect server
cd prefect
prefect server start
```
