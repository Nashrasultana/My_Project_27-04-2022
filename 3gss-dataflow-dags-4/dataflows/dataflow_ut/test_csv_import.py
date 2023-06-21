import os
import unittest
import apache_beam as beam

from ..dataflow_templates.csv_import_job import run, UserOptions, convert_to_dictionary, remove_headers
from ..dataflow_templates.null_primary_key_transform import NullPrimaryKeyTransform
from ..dataflow_templates.dictionary_to_csv import DictToCSV
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.value_provider import StaticValueProvider


input_filepath = os.path.join(os.path.dirname(
    __file__), "test_resources/test_valid_input.csv.gz")
input_filepath_null_pk = os.path.join(os.path.dirname(
    __file__), "test_resources/test_null_pk_input.csv")
csv_output_filepath = os.path.join(os.path.dirname(
    __file__), "test_resources/test_valid_uncompressed_output")
output_filepath = os.path.join(os.path.dirname(
    __file__), "test_resources/test_valid_output")
expected_output_filepath = os.path.join(os.path.dirname(
    __file__), "test_resources/test_expected_valid_output.csv")
expected_columns_removed_output_filepath = os.path.join(os.path.dirname(
    __file__), "test_resources/test_expected_valid_columns_removed_output.csv")


class CsvImportTests(unittest.TestCase):  
    def test_dataflow_job_pk_null_no_null_values_output(self):
        expected_output = [
            {"name": "Bob", "age": "34", "description": "test"},
            {"name": "Geoff", "age": "66", "description": "another test"}
        ]

        headers = ["name", "age", "description"]

        with TestPipeline() as p:
            input = p | beam.io.ReadFromText(
                file_pattern=input_filepath, skip_header_lines=1)

            dictionary = input | beam.Map(convert_to_dictionary, headers)
            dictionary = dictionary | beam.Map(remove_headers, StaticValueProvider(str, ""))
            pk_null_output = dictionary | beam.Map(
                NullPrimaryKeyTransform().process, "name")
            pk_null_output = pk_null_output | beam.Filter(lambda x: x != None)

            assert_that(
                pk_null_output,
                equal_to(expected_output)
            )

    def test_dataflow_job_map_to_object(self):
        expected_output = [
            {"name": "Bob", "age": "34", "description": "test"},
            {"name": "Geoff", "age": "66", "description": "another test"}
        ]

        headers = ["name", "age", "description"]

        with TestPipeline() as p:
            input = p | beam.io.ReadFromText(
                file_pattern=input_filepath, compression_type='gzip', skip_header_lines=1)
            result = input | beam.Map(convert_to_dictionary, headers)

            assert_that(
                result,
                equal_to(expected_output)
            )

    def test_dataflow_job_remove_columns(self):
        expected_output = ["Bob,34", "Geoff,66"]

        original_headers = ["name", "age", "description"]
        new_headers = ["name", "age"]

        with TestPipeline() as p:
            input = p | beam.io.ReadFromText(
                file_pattern=input_filepath, compression_type='gzip', skip_header_lines=1)

            dictionary = input | beam.Map(
                convert_to_dictionary, original_headers)
            dictionary = dictionary | beam.Map(remove_headers, StaticValueProvider(str, "description"))
            result = dictionary | DictToCSV(new_headers)

            assert_that(
                result,
                equal_to(expected_output)
            )

    def test_dataflow_job_pk_null_with_null_values_output(self):
        expected_output = [
            {'name': 'Geoff', 'age': '66', 'description': 'another test'}]
        headers = ["name", "age", "description"]

        with TestPipeline() as p:
            input = p | beam.io.ReadFromText(
                file_pattern=input_filepath_null_pk, skip_header_lines=1)

            dictionary = input | beam.Map(convert_to_dictionary, headers)
            dictionary = dictionary | beam.Map(remove_headers, StaticValueProvider(str, ""))
            pk_null_output = dictionary | beam.Map(
                NullPrimaryKeyTransform().process, "name")
            pk_null_output = pk_null_output | beam.Filter(lambda x: x != None)

            assert_that(
                pk_null_output,
                equal_to(expected_output)
            )

    def test_dataflow_job_runs_e2e_with_default_options(self):
        pipeline_options = PipelineOptions().from_dictionary({
            "input": input_filepath,
            "output": output_filepath,
            "input_file_headers": "NAME,AGE,DESC",
            "output_file_header": "NAME,AGE,DESC"
        })

        user_options = pipeline_options.view_as(UserOptions)

        with TestPipeline(options=user_options) as p:
            result = run(p)
            assert result.state == "DONE"

        # Assert that the file exists and contains the expected data
        expected_output_file = open(expected_output_filepath).read()
        actual_output_file = open(
            output_filepath + "-00000-of-00001.csv").read()
        assert actual_output_file == expected_output_file

    def test_dataflow_job_runs_e2e(self):
        pipeline_options = PipelineOptions().from_dictionary({
            "input": input_filepath,
            "output": output_filepath,
            "input_file_headers": "NAME,AGE,DESC",
            "output_file_header": "NAME,AGE",
            "primary_key": "NAME",
            "columns_to_remove": "DESC"
        })

        user_options = pipeline_options.view_as(UserOptions)

        with TestPipeline(options=user_options) as p:
            result = run(p)
            assert result.state == "DONE"

        # Assert that the file exists and contains the expected data
        expected_output_file = open(expected_columns_removed_output_filepath).read()
        actual_output_file = open(
            output_filepath + "-00000-of-00001.csv").read()
        assert actual_output_file == expected_output_file

    # Cleanup output file after full run
    @classmethod
    def tearDownClass(cls):
        os.remove(output_filepath + "-00000-of-00001.csv")