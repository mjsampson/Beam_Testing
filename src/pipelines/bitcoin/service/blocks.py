import logging
import requests
import urllib
import os
from apache_beam.io import iobase, OffsetRangeTracker
from apache_beam.metrics import Metrics
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import PTransform
import apache_beam as beam
from config.config import DefaultConfig

from py_bitcoin import BitcoinReader

bitcoin = BitcoinReader(rpcString=DefaultConfig.BITCOIN_RPCUSER + ":" + DefaultConfig.BITCOIN_RPCPASSWORD + '@' + DefaultConfig.BITCOIN_HOST)

class _BitcoinBlocks(iobase.BoundedSource):
    def __init__(self, count):
        self.blocks = Metrics.counter(self.__class__, 'blocks')
        self._count = count

    def estimate_size(self):
        return self._count

    def get_range_tracker(self, start_postion, stop_position):
        if start_postion is None:
            start_postion = 0
        if stop_position is None:
            stop_position = self._count

        return OffsetRangeTracker(start_postion, stop_position)

    def read(self, range_tracker):
        for i in range(range_tracker.start_position(),
                       range_tracker.stop_position()):
            if not range_tracker.try_claim(i):
                return
            self.blocks.inc()
            yield bitcoin.getBlk(i)['result']


    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = 0

        bundle_start = start_position
        while bundle_start < stop_position:
            bundle_stop = min(stop_position, bundle_start + desired_bundle_size)
            yield iobase.SourceBundle(weight=(bundle_stop - desired_bundle_size),
                                      source=self,
                                      start_position=bundle_start,
                                      stop_position=bundle_stop)
            bundle_start = bundle_stop

class ReadBitcoinBlocks(PTransform):

    def __init__(self, count):
        super(ReadBitcoinBlocks, self).__init__()
        self._count = count

    def expand(self, pcoll):
        return pcoll | iobase.Read(_BitcoinBlocks(self._count))

