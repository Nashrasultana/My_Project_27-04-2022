import logging

from dataflow_templates import csv_import_job

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  csv_import_job.run_pipeline()