#!/bin/sh

python ./sub2bq.py \
    --output_table YOUR_BQ_TABLE \
    --input_subscription YOUR_SUBSCRIBE \
    --window_interval 10 \
    --create_if_missing 1 \
    --worker_machine_type='n1-standard-1' \
    --requirements_file ./requirements.txt