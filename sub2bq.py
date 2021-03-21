import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window


# Defines the BigQuery schema for the output table.
SCHEMA = ','.join([
    'msg:STRING'
])


def parse_json_message(message):
    """Parse the input json message."""
    row = json.loads(message)
    return {
        'msg': row['msg']
    }


def run(args, input_subscription, output_table, window_interval):
    """Build and run the pipeline."""
    options = PipelineOptions(
        args, save_main_session=True, streaming=True, runner='DataflowRunner',
        project='YOUR_PROJECT', job_name='YOUR_JOB_NAME', 
        temp_location='YOUR_BUCKET', region='YOUR_REGION'
    )



    with beam.Pipeline(options=options) as pipeline:
        messages = (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
                subscription=input_subscription).with_output_types(bytes)
            | 'UTF-8 bytes to string' >> beam.Map(lambda msg: msg.decode('utf-8'))
            | 'Parse JSON messages' >> beam.Map(parse_json_message))

        # Output the results into BigQuery table.
        _ = messages | 'Write to Big Query' >> beam.io.WriteToBigQuery(
            output_table, schema=SCHEMA)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_table',
        default=None, 
        help='Output BigQuery table for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.'
    )
    parser.add_argument(
        '--input_subscription',
        default=None, 
        help='Input PubSub subscription of the form: "projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'
    )
    parser.add_argument(
        '--window_interval',
        default=60,
        help='Window interval in seconds for grouping incoming messages.'
    )
    
    known_args, pipeline_args = parser.parse_known_args()
    
    run(pipeline_args, known_args.input_subscription, 
        known_args.output_table, known_args.window_interval)
