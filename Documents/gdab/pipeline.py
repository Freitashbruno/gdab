import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode, Repeatedly
import json
import logging

logging.basicConfig(level=logging.INFO)

def run():
    options = PipelineOptions(
        project="gdab-430616",  # ID do projeto
        runner="DataflowRunner",  # Runner do Dataflow
        temp_location="gs://gdab/temp",  # Nome do bucket
        region="us-central1"
    )

    def parse_json(element):
        logging.info(f'Parsing element: {element}')
        record = json.loads(element)
        return {
            'id': record['id'],
            'name': record['name'],
            'email': record['email'],
            'timestamp': record['timestamp']
        }

    with beam.Pipeline(options=options) as p:
        # Ler mensagens do Pub/Sub
        messages = (p 
                    | 'Ler mensagens do Pub/Sub' >> beam.io.ReadFromPubSub(subscription='projects/gdab-430616/subscriptions/gdab')
                    | 'Parsear JSON' >> beam.Map(parse_json)
                    | 'Aplicar Janelas' >> beam.WindowInto(
                        FixedWindows(60),
                        trigger=Repeatedly(AfterProcessingTime(1)),
                        accumulation_mode=AccumulationMode.DISCARDING)
                   )

        # Salvar no BigQuery
        messages | 'Salvar no BigQuery' >> beam.io.WriteToBigQuery(
                        'gdab-430616:gdab.gdab',
                        schema='id:STRING, name:STRING, email:STRING, timestamp:TIMESTAMP',
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                    )

if __name__ == '__main__':
    run()
