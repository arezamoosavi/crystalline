import argparse
import pandas as pd
import apache_beam as beam

from apache_beam.io import WriteToText

from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.options.pipeline_options import PipelineOptions

arg_parser = argparse.ArgumentParser()
_, beam_args = arg_parser.parse_known_args()
beam_options = PipelineOptions(beam_args)


class AppOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input_file')

        parser.add_value_provider_argument('--output_file')


class ReadDF(beam.DoFn):
    """
        It simply returns the value of the DF
    """

    def __init__(self, input_file: RuntimeValueProvider):
        beam.DoFn.__init__(self)
        self.input_file = input_file

    def process(self, *args, **kwargs):
        pandas_df = pd.read_csv(self.input_file.get(),
                                sep='|',
                                encoding='utf-16',
                                error_bad_lines=True,
                                iterator=True,
                                chunksize=100000,
                                low_memory=False,
                                warn_bad_lines=True,
                                verbose=True)
        return pandas_df


with beam.Pipeline(options=beam_options) as p:
    app_options = beam_options.view_as(AppOptions)

    (p
     | 'Start' >> beam.Create([''])
     | 'Read Df file path' >> beam.ParDo(ReadDF(app_options.input_file))
     | 'Reshuffle' >> beam.Reshuffle()
     | 'Export results' >> WriteToText(app_options.output_file, '.txt'))
