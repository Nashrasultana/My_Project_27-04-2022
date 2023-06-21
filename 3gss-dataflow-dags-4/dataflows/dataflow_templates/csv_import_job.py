import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
try:
    from .null_primary_key_transform import NullPrimaryKeyTransform
except:
    from null_primary_key_transform import NullPrimaryKeyTransform
try:
    from .dictionary_to_csv import DictToCSV
except:
    from dictionary_to_csv import DictToCSV
import logging


class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--templated_int', type=int)
        parser.add_value_provider_argument("--input", type=str)
        parser.add_value_provider_argument("--output", type=str)
        parser.add_value_provider_argument("--input_file_headers", type=str)
        parser.add_value_provider_argument("--output_file_header", type=str)
        parser.add_value_provider_argument("--primary_key", type=str, default="")
        parser.add_value_provider_argument("--columns_to_remove", type=str, default="")
        parser.add_value_provider_argument("--delimeter", type=str, default=",")


class process_file(beam.DoFn):
    def __init__(self, templated_int):
        self.templated_int = templated_int

    def process(self, an_int):
        yield self.templated_int.get() + an_int


def remove_headers(element, headers_to_remove):
    headers = headers_to_remove.get()
    if headers == "":
        return element

    for header in headers.split(","):
        try:
            del element[header]
        except:
            logging.info("Header does not exist, unable to remove header " + header)
    return element


def convert_to_dictionary(element, headers, delimeter=","):
    csv_dictionary = {}
    counter: int = 0

    for cell in element.split(delimeter):
        try:
            csv_dictionary[headers[counter]] = cell
            counter = counter + 1
        except:
            logging.error("Failed to convert to Dictionary. Headers: " + str(headers))
    return csv_dictionary


def run_pipeline():
    """Main pipeline for item_file. Takes user input arguments and performs transformation """
    pipeline_options = PipelineOptions()
    user_options = pipeline_options.view_as(UserOptions)

    with beam.Pipeline(options=user_options) as p:
        run(p)

    logging.info('Pipeline completed successfully')
    p.run().wait_until_finish()


def run(p):
    """Main pipeline for item_file. Takes user input arguments and performs transformation """
    logging.info('Pipeline has started running.')
    (p
        | 'Read from a File' >> beam.io.ReadFromText(file_pattern=p._options.input, compression_type="gzip", skip_header_lines=1)
        | 'Parse csv to dictionary' >> beam.Map(convert_to_dictionary, p._options.input_file_headers.get().split(","), p._options.delimeter.get())
        | 'Remove unwanted headers' >> beam.Map(remove_headers, p._options.columns_to_remove)
        | 'Primary key null check' >> beam.Map(NullPrimaryKeyTransform().process, p._options.primary_key.get())
        | 'Remove any None values' >> beam.Filter(lambda x: x != None)
        | 'Convert dictionary back to a csv string' >> DictToCSV(p._options.output_file_header.get().split(","), p._options.delimeter.get())
        | 'Write to clean file' >> beam.io.WriteToText(p._options.output, file_name_suffix=".csv", header=p._options.output_file_header)
     )

    result = p.run()
    result.wait_until_finish()
    logging.info('Pipeline completed successfully')
    return result
