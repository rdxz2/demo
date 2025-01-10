source ~/venv/prefect/bin/activate

prefect profile use ephemeral
prefect server start --host 0.0.0.0
