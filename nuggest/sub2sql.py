import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import relational_db


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--drivername', dest='drivername', default='sqlite')
    parser.add_argument('--host', dest='host', default=None)
    parser.add_argument('--port', type=int, dest='port', default=None)
    parser.add_argument('--database', dest='database', default='/tmp/test.db')
    parser.add_argument('--username', dest='username', default=None)
    parser.add_argument('--password', dest='password', default=None)
    parser.add_argument('--create_if_missing', type=bool, dest='create_if_missing', default=None)
    parser.add_argument('--input_subscription', default=None, help='Input PubSub subscription of the form "projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."')

    db_args, pipeline_args = parser.parse_known_args()

    return vars(db_args), pipeline_args


def parse_json_message(message):
    """Parse the input json message and add 'score' & 'processing_time' keys."""
    row = json.loads(message)
    return {
        'msg': row['msg']
    }


def run(**db_args, pipeline_args):

    source_config = relational_db.SourceConfiguration(
        drivername=db_args['drivername'], 
        host=db_args['host'], 
        port=db_args['port'], 
        database=db_args['database'], 
        username=db_args['username'], 
        password=db_args['password'], 
        create_if_missing=db_args['create_if_missing']
    )

    table_config = relational_db.TableConfiguration(
        name='YOUR_TABLE_NAME', # table name
        create_if_missing=True,  # automatically create the table if not there
        primary_key_columns=['id']
    )


    """Build and run the pipeline."""
    options = PipelineOptions(
        pipeline_args, save_main_session=True, streaming=True, runner='DataflowRunner',
        project='YOUR_PROJECT', job_name='YOUR_JOB', temp_location='YOUR_BUCKET', 
        region='YOUR_REGION'
    )


    with beam.Pipeline(options=options) as pipeline:
        messages = (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
                subscription=kwargs['input_subscription']).with_output_types(bytes)
            | 'UTF-8 bytes to string' >> beam.Map(lambda msg: msg.decode('utf-8'))
            | 'Parse JSON messages' >> beam.Map(parse_json_message))

        # Output the results into Cloud SQL table.
        _ = messages | 'Write to Cloud SQL' >> relational_db.Write(
            source_config=source_config,
            table_config=table_config
        )


if __name__ == '__main__':
    db_args, pipeline_args = get_args()
    run(**db_args, pipeline_args)
