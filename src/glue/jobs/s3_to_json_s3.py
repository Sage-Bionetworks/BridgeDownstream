# Breaks apart the archive files into their own directories
# so that the schema (specific to the taskIdentifier) can be maintained
# by a Glue crawler.
import io
import os
import sys
import json
import zipfile
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions

glue_client = boto3.client("glue")
s3_client = boto3.client("s3")

args = getResolvedOptions(
        sys.argv,
        ["WORKFLOW_NAME",
         "WORKFLOW_RUN_ID"])
workflow_run_properties = glue_client.get_workflow_run_properties(
        Name=args["WORKFLOW_NAME"],
        RunId=args["WORKFLOW_RUN_ID"])["RunProperties"]

def process_record(s3_obj, s3_obj_metadata):
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
                        workflow_run_properties["json_prefix"],
                        f"dataset={dataset_name}",
                        f"taskIdentifier={s3_obj_metadata['taskidentifier']}",
                        f"year={str(created_on.year)}",
                        f"month={str(created_on.month)}",
                        f"day={str(created_on.day)}",
                        f"recordId={s3_obj_metadata['recordid']}",
                        output_fname)
                with open(output_path, "rb") as f_in:
                    response = s3_client.put_object(
                            Body = f_in,
                            Bucket = workflow_run_properties["json_bucket"],
                            Key = s3_output_key,
                            Metadata = s3_obj_metadata)


s3_obj = s3_client.get_object(
        Bucket = workflow_run_properties["source_bucket"],
        Key = workflow_run_properties["source_key"])
process_record(
        s3_obj = s3_obj,
        s3_obj_metadata=s3_obj["Metadata"])
