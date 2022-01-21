# inspect test data -- look for schema deviation
import json
import os
import sys
from pathlib import Path
from zipfile import ZipFile

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data_dir = '/Users/tthyer/github/Sage-Bionetworks/BridgeDownstream/src/scripts/setup_test_data/data'
zip_files = (item for item in Path(data_dir).iterdir() if item.is_file())
has_schema_identifier = []
no_schema_identifier = []
taskdata_filename = 'taskData.json'
field_name = 'fnlscore'
for file_path in zip_files:
  zip_filename = file_path.parts[-1]
  with ZipFile(file_path) as archive:
    archive.extract(taskdata_filename)
    df = spark.read.option("multiline",True).json(taskdata_filename)
    schema = df.schema.simpleString() #df.schema.names
    file_list = has_schema_identifier if field_name in schema else no_schema_identifier
    file_list.append(zip_filename)
    os.remove(taskdata_filename)
print(f'taskData.json has $field_name')
print('--------------------------------------------')
for path in has_schema_identifier:
  print(path)
print('\ntaskData.json missing $field_name')
print('--------------------------------------------')
for path in no_schema_identifier:
  print(path)
