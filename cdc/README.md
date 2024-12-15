# Virtual env installation

```sh
conda create -n xz2democdc312 python=3.12 -y
conda activate xz2democdc312
pip install -r requirements.txt
```

# Running the service locally

```sh
docker compose up --build
```

# Deploying

## Building docker image

```sh
docker build -t xz2-demo-cdc-streamer:v0.0.1 Dockerfile.streamer
docker build -t xz2-demo-cdc-uploader:v0.0.1 Dockerfile.uploader
```
