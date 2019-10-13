import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from pipelines.bitcoin.service.blocks import ReadBitcoinBlocks


with Pipeline(options = PipelineOptions()) as p:
    numbers = p | 'GetBlocks' >> ReadBitcoinBlocks(100)
    numbers | "WriteToText" >> beam.io.textio.WriteToText("test.txt")