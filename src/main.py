import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from pipelines.bitcoin.service.blocks import ReadBitcoinBlocks
import time
start_time = time.time()
print("Reading Blocks")
with Pipeline(options = PipelineOptions()) as p:
    numbers = p | 'GetBlocks' >> ReadBitcoinBlocks(100)
    numbers | "WriteToText" >> beam.io.textio.WriteToText("test.txt")
print("Writing Blocks done. Total Time " + str(time.time() - start_time) + " seconds")
