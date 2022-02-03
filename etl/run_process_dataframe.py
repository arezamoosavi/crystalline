import apache_beam as beam
import argparse

from apache_beam.io import WriteToText
from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe import convert

from apache_beam.options.pipeline_options import PipelineOptions

arg_parser = argparse.ArgumentParser()
_, beam_args = arg_parser.parse_known_args()
beam_options = PipelineOptions(beam_args)


class AppOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input_file')
        parser.add_value_provider_argument('--output_file')


with beam.Pipeline(options=beam_options) as p:
    app_options = beam_options.view_as(AppOptions)

    beam_df = p | 'Read CSV' >> read_csv(path=app_options.input_file.get(),
                                         splittable=True,
                                         sep='|',
                                         iterator=True,
                                         chunksize=100000,
                                         low_memory=False,
                                         verbose=True)
    (
        # Convert the Beam DataFrame to a PCollection.
        convert.to_pcollection(beam_df, yield_elements='pandas')

        # Reshuffle to make sure parallelization is balanced.
        | 'Reshuffle' >> beam.Reshuffle()

        # We get named tuples, we can convert them to dictionaries like this.
        # | 'To dictionaries' >> beam.Map(lambda x: dict(x._asdict()))

        # Print the elements in the PCollection.
        # | 'Print' >> beam.Map(print)
        | 'Export results to new file' >> WriteToText(app_options.output_file,
                                                      '.txt'))
