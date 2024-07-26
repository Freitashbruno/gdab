import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
import json

def run():
    options = PipelineOptions(
        project="gdab-430616",  # ID do projeto
        runner="DataflowRunner",
        temp_location="gs://gdab/temp",  # Nome do bucket
        region="us-central1"
    )

    def parse_json(element):
        record = json.loads(element)
        return {
            'id': record['id'],
            'name': record['name'],
            'email': record['email'],
            'timestamp': record['timestamp']
        }

    with beam.Pipeline(options=options) as p:
        (p 
         | 'Ler mensagens do Pub/Sub' >> beam.io.ReadFromPubSub(subscription='projects/gdab-430616/subscriptions/gdab')
         | 'Parsear JSON' >> beam.Map(parse_json)
         | 'Aplicar Janelas' >> beam.WindowInto(FixedWindows(60))  # Aplicar janelas de 1 minuto
         | 'Salvar no BigQuery' >> beam.io.WriteToBigQuery(
             'gdab-430616:gdab.gdab',
             schema='id:STRING, name:STRING, email:STRING, timestamp:TIMESTAMP',
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
         ))

if __name__ == '__main__':
    run()
