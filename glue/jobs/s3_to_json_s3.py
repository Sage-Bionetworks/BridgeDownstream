# Breaks apart the archive files into their own directories
# so that the schema (specific to the taskIdentifier) can be maintained
# by a Glue crawler.
import io
import os
import sys
import csv
import time
import json
import zipfile
import boto3
from datetime import datetime
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
        sys.argv,
        ["input-bucket",
         "input-prefix",
         "output-bucket",
         "output-prefix", # e.g. app/study/ndjson/
         "glue-diff-database", # do not process the records here
         "glue-diff-table", # because we have already processed them!
         "glue-diff-interval"]) # num past days to check for processed record

s3_client = boto3.client("s3")
athena_client = boto3.client("athena")

def get_already_processed_records(glue_database, glue_table, day_interval):
    output_bucket = args["output_bucket"]
    output_prefix = "tmp/"
    query_str = (
            f"select distinct recordId from {glue_table}"
            f"where cast(createdOn as double) > "
            f"to_unixtime("
            f"current_timestamp - INTERVAL '{day_interval}' DAY)*1000")
    query = athena_client.start_query_execution(
            QueryString = f"select distinct recordId from {glue_table}",
            QueryExecutionContext={
                "Database": glue_database,
                "Catalog": "AwsDataCatalog"},
            ResultConfiguration={
                "OutputLocation": f"s3://f{output_bucket}/f{output_prefix}"})
    for i in range(10):
        query_execution = athena_client.get_query_execution(
                QueryExecutionId = query["QueryExecutionId"])
        query_state = query_execution["QueryExecution"]["Status"]["State"]
        if query_state == "SUCEEDED":
            break
        elif query_state == "QUEUED":
            time.sleep(1)
            continue
        else: # either "FAILED", "CANCELLED", or something we didn't expect
            return set()
    fpath = f"{query['QueryExecutionId']}.csv"
    try:
        s3_client.download_file(
                Bucket = output_bucket,
                Key = os.path.join(output_prefix, fpath)
                Filename = fpath)
    except s3_client.exceptions.ClientError:
        return(set())
    already_processed_records = set()
    with open(fpath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            already_processed_records.add(row["recordId"])
    return(already_processed_records)

s3_objects = s3_client.list_objects_v2(
        Bucket = args["input_bucket"],
        Prefix = args["input_prefix"])
already_processed_records = get_already_processed_records(
        glue_database = args["glue_diff_database"]
        glue_table = args["glue_diff_table"],
        day_interval = args["glue_diff_interval"])
for s3_obj in s3_objects["Contents"]:
    s3_obj = s3_client.get_object(
            Bucket = args["input_bucket"],
            Key = s3_obj["Key"])
    s3_obj_metadata = s3_obj["Metadata"]
    if s3_obj_metadata["recordid"] in already_processed_records:
        continue
    created_on = datetime.fromtimestamp(
            int(s3_obj_metadata["createdon"]) / 1000)
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"].read())) as z:
        contents = z.namelist()
        for json_path in z.namelist():
            dataset_name = os.path.splitext(json_path)[0]
            os.makedirs(dataset_name, exist_ok=True)
            with z.open(json_path, "r") as p:
                j = json.load(p)
                # We inject all S3 metadata into the metadata file
                if dataset_name == "metadata":
                    j["year"] = int(created_on.year)
                    j["month"] = int(created_on.month)
                    j["day"] = int(created_on.day)
                    for key in s3_obj_metadata:
                        # We revert partition fields back to camelCase
                        if key == "taskidentifier":
                            j["taskIdentifier"] = s3_obj_metadata[key]
                        elif key == "recordid":
                            j["recordId"] = s3_obj_metadata[key]
                        else:
                            j[key] = s3_obj_metadata[key]
                else: # but only the partition fields into other files
                    if type(j) == list:
                        for item in j:
                            item["taskIdentifier"] = s3_obj_metadata["taskidentifier"]
                            item["year"] = int(created_on.year)
                            item["month"] = int(created_on.month)
                            item["day"] = int(created_on.day)
                            item["recordId"] = s3_obj_metadata["recordid"]
                    else:
                        j["taskIdentifier"] = s3_obj_metadata["taskidentifier"]
                        j["year"] = int(created_on.year)
                        j["month"] = int(created_on.month)
                        j["day"] = int(created_on.day)
                        j["recordId"] = s3_obj_metadata["recordid"]
                output_fname = s3_obj_metadata["recordid"] + ".ndjson"
                output_path = os.path.join(dataset_name, output_fname)
                with open(output_path, "w") as f_out:
                    json.dump(j, f_out, indent=None)
                s3_output_key = os.path.join(
                        args["output_prefix"],
                        dataset_name,
                        str(created_on.year),
                        str(created_on.month),
                        str(created_on.day),
                        output_fname)
                with open(output_path, "rb") as f_in:
                    response = s3_client.put_object(
                            Body = f_in,
                            Bucket = args["output_bucket"]
                            Key = s3_output_key,
                            Metadata = s3_obj_metadata)


