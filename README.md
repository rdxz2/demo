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

# Run prefect server
cd prefect
prefect server start
```
