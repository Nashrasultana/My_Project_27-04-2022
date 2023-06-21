import apache_beam as beam
import logging


# Custom logger 
logger = logging.getLogger('csv_import_job_logger')
logger.setLevel(logging.INFO)

"""Custom Beam DoFn that will check for null primary keys. If the primary key is null the record will be ignored.

Args:
    element (list): List of strings that represents a row in from a csv file. The first item in said list is assumed to be the primary key. 

Returns:
    list: a list of lists representing the valid rows of the csv file that have a primary key.
"""
class NullPrimaryKeyTransform(beam.DoFn):
  def process(self, element, primary_key):
      return self.pk_null_check(element, primary_key)

  def pk_null_check(self, element, primary_key):
    try:
        if element[primary_key] == "" or element[primary_key] == None:
            logger.error(f"Null value in field: " + primary_key, exc_info=True)
            logger.info('Primary key is null')
        else:
            logger.info('Primary key is not null')
            return element
    except:
        logger.warning('Provided primary key does not exist: {' + primary_key + '}' )
        return element