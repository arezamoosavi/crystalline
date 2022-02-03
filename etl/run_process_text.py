import apache_beam as beam
import argparse

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

from apache_beam.options.pipeline_options import PipelineOptions

arg_parser = argparse.ArgumentParser()
_, beam_args = arg_parser.parse_known_args()
beam_options = PipelineOptions(beam_args)

class AppOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input_file')
        parser.add_value_provider_argument(
            '--output_file')


with beam.Pipeline(options=beam_options) as p:
    app_options = beam_options.view_as(AppOptions)

    data_from_source = (p
                        | 'ReadMyFile' >> ReadFromText(app_options.input_file)
                        | 'Splitter using beam.Map' >> beam.Map(lambda record: (record.split('|'))[0])
                        | 'Export results to new file' >> WriteToText(app_options.output_file, '.txt')
                        )

    p.run().wait_until_finish()