#!/bin/sh

python test.py \
    --drivername 'postgresql+pg8000' \
    --host 'YOUR_HOST' \
    --port '5432' \
    --database 'dataflowdb' \
    --username 'postgres' \
    --password 'postgres' \
    --create_if_missing 1 \
    --worker_machine_type='n1-standard-1' \
    --requirements_file ../requirements.txt
