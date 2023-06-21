import csv
from csv import DictWriter
from csv import excel
from io import StringIO
import apache_beam as beam

def _dict_to_csv(element, column_order, missing_val='', discard_extras=True, dialect=excel):
    """ Additional properties for delimiters, escape chars, etc via an instance of csv.Dialect
        Note: This implementation does not support unicode
    """

    if element != None:
        buf = StringIO()

        writer = DictWriter(buf,
                            fieldnames=column_order,
                            restval=missing_val,
                            extrasaction=('ignore' if discard_extras else 'raise'),
                            dialect=dialect)
        writer.writerow(element)

        return buf.getvalue().rstrip(dialect.lineterminator)


class _DictToCSVFn(beam.DoFn):
    """ Converts a Dictionary to a CSV-formatted String

        column_order: A tuple or list specifying the name of fields to be formatted as csv, in order
        missing_val: The value to be written when a named field from `column_order` is not found in the input element
        discard_extras: (bool) Behavior when additional fields are found in the dictionary input element
        dialect: Delimiters, escape-characters, etc can be controlled by providing an instance of csv.Dialect

    """

    def __init__(self, column_order, missing_val='', discard_extras=True, dialect=excel):
        self._column_order = column_order
        self._missing_val = missing_val
        self._discard_extras = discard_extras
        self._dialect = dialect

    def process(self, element, *args, **kwargs):
        result = _dict_to_csv(element,
                              column_order=self._column_order,
                              missing_val=self._missing_val,
                              discard_extras=self._discard_extras,
                              dialect=self._dialect)

        return [result,]

class DictToCSV(beam.PTransform):
    """ Transforms a PCollection of Dictionaries to a PCollection of CSV-formatted Strings

        column_order: A tuple or list specifying the name of fields to be formatted as csv, in order
        missing_val: The value to be written when a named field from `column_order` is not found in an input element
        discard_extras: (bool) Behavior when additional fields are found in the dictionary input element
        dialect: Delimiters, escape-characters, etc can be controlled by providing an instance of csv.Dialect

    """

    def __init__(self, column_order, missing_val='', discard_extras=True, dialect=excel):
        self._column_order = column_order
        self._missing_val = missing_val
        self._discard_extras = discard_extras
        self._dialect = dialect

    def expand(self, pcoll, delimeter=","):
        if delimeter == "|":
            self._dialect = pipe
        return pcoll | beam.ParDo(_DictToCSVFn(column_order=self._column_order,
                                          missing_val=self._missing_val,
                                          discard_extras=self._discard_extras,
                                          dialect=self._dialect)
                             )

class pipe(csv.Dialect):
    """Describe the usual properties of Excel-generated CSV files."""
    delimiter = '|'
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\r\n'
    quoting = csv.QUOTE_MINIMAL
csv.register_dialect("pipe", pipe)