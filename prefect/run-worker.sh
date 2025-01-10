source ~/venv/prefect/bin/activate

prefect profile use local
prefect worker start --pool work-pool-2
