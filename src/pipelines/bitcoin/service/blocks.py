import logging
import requests
import urllib
import os
from apache_beam.io import iobase, OrderedPositionRangeTracker
from apache_beam.metrics import Metrics
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import PTransform
import apache_beam as beam
from config.config import DefaultConfig
import logging
from py_bitcoin import BitcoinReader
from past.builtins import long
import threading
bitcoin = BitcoinReader(rpcString=DefaultConfig.BITCOIN_RPCUSER + ":" + DefaultConfig.BITCOIN_RPCPASSWORD + '@' + DefaultConfig.BITCOIN_HOST)



class BitcoinBatchRanges(beam.DoFn):
    def __init__(self, start, end, batchSize):
        if start is None:
            raise ValueError('Start offset must not be \'None\'')
        if end is None:
            raise ValueError('End offset must not be \'None\'')
        if batchSize is None:
            raise ValueError('Batch Size must not be \'None\'')

        assert start <= end

        self._start = start
        self._end = end
        self._batchSize = batchSize

    def process(self, val):
        length = self._end - self._start
        tasks = int(length / self._batchSize)

        for i in range(tasks):
            start_pos = self._start + i * self._batchSize
            yield(start_pos, start_pos+self._batchSize)
        if length % self._batchSize != 0:
            start_pos = self._start + tasks * self._batchSize
            yield (start_pos, start_pos + length % self._batchSize)




# class ReadBitcoinBlocks(PTransform):
#
#     def __init__(self, count):
#         super(ReadBitcoinBlocks, self).__init__()
#         self._count = count
#
#     def expand(self, pcoll):
#         return pcoll | iobase.Read(_BitcoinBlocks(self._count))

if __name__ == "__main__":
    with beam.Pipeline(options = PipelineOptions()) as p:
        number = (p | "random" >> beam.Create([1])
        | "Ranges" >> beam.ParDo(BitcoinBatchRanges(0, 5670000, 1000))
        | beam.Map(print))
        # numbers = (p | "Ranges" >> beam.ParDo(BitcoinBatchRanges(0, 10000, 1000)))
        # numbers | "WriteToText" >> beam.io.textio.WriteToText("test.txt")