# Breaks apart the archive files into their own directories
# so that the schema (specific to the taskIdentifier) can be maintained
# by a Glue crawler.
# Takes about 12 seconds per archive (assuming archive ~ 1 MB)
import io
import os
import sys
import json
import zipfile
import boto3
from datetime import datetime
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
        sys.argv,
        ["bucket",
         "prefix"])

client = boto3.client("s3")

s3_objects = client.list_objects_v2(
        Bucket = args["bucket"],
        Prefix = args["prefix"])
for s3_obj in s3_objects["Contents"]:
    s3_obj = client.get_object(
            Bucket = args["bucket"],
            Key = s3_obj["Key"])
    s3_obj_metadata = s3_obj["Metadata"]
    created_on = datetime.fromtimestamp(
            int(s3_obj_metadata["createdon"]) / 1000)
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"].read())) as z:
        contents = z.namelist()
        for json_path in z.namelist():
            dataset_name = os.path.splitext(json_path)[0]
            with z.open(json_path, "r") as p:
                j = json.load(p)
                # We inject all S3 metadata into the metadata file
                if dataset_name == "metadata":
                    j["year"] = created_on.year
                    j["month"] = created_on.month
                    j["day"] = created_on.day
                    for key in s3_obj_metadata:
                        # We revert partition fields back to camelCase
                        if key == "taskidentifier":
                            j["taskIdentifier"] = s3_obj_metadata[key]
                        if key == "recordid":
                            j["recordId"] = s3_obj_metadata[key]
                        else:
                            j[key] = s3_obj_metadata[key]
                else: # but only the partition fields into other files
                    if type(j) == list:
                        for item in j:
                            item["taskIdentifier"] = s3_obj_metadata["taskidentifier"]
                            item["year"] = created_on.year
                            item["month"] = created_on.month
                            item["day"] = created_on.day
                            item["recordId"] = s3_obj_metadata["recordid"]
                    else:
                        j["taskIdentifier"] = s3_obj_metadata["taskidentifier"]
                        j["year"] = created_on.year
                        j["month"] = created_on.month
                        j["day"] = created_on.day
                        j["recordId"] = s3_obj_metadata["recordid"]
                output_fname = s3_obj_metadata["recordid"] + ".ndjson"
                output_path = os.path.join(dataset_name, output_fname)
                os.makedirs(dataset_name, exist_ok=True)
                with open(output_path, "w") as f_out:
                    json.dump(j, f_out, indent=None)
                s3_output_bucket = args["bucket"]
                s3_output_prefix = "raw_json"
                s3_output_key = os.path.join(
                        s3_output_prefix, dataset_name, output_fname)
                with open(output_path, "rb") as f_in:
                    response = client.put_object(
                            Body = f_in,
                            Bucket = s3_output_bucket,
                            Key = s3_output_key,
                            Metadata = s3_obj_metadata)


