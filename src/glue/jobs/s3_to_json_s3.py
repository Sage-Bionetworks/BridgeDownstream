# Breaks apart the archive files into their own directories
# so that the schema (specific to the taskIdentifier) can be maintained
# by a Glue crawler.
import io
import json
import logging
import os
import sys
import zipfile
from datetime import datetime
from urllib.parse import urlparse

import boto3
from awsglue.utils import getResolvedOptions


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

glue_client = boto3.client("glue")
s3_client = boto3.client("s3")

args = getResolvedOptions(
        sys.argv,
        ["WORKFLOW_NAME",
         "WORKFLOW_RUN_ID",
         "scriptLocation"])
workflow_run_properties = glue_client.get_workflow_run_properties(
        Name=args["WORKFLOW_NAME"],
        RunId=args["WORKFLOW_RUN_ID"])["RunProperties"]

def get_dataset_mapping(script_location):
    script_location = urlparse(script_location)
    dataset_mapping_bucket = script_location.netloc
    dataset_mapping_key = "BridgeDownstream/main/glue/resources/dataset_mapping.json"
    dataset_mapping_fname = os.path.basename(dataset_mapping_key)
    dataset_mapping_file = s3_client.download_file(
            Bucket=dataset_mapping_bucket,
            Key=dataset_mapping_key,
            Filename=dataset_mapping_fname)
    with open(dataset_mapping_fname, "r") as f:
        dataset_mapping = json.load(f)
    return(dataset_mapping)


def process_record(s3_obj, s3_obj_metadata, dataset_mapping):
    created_on = datetime.fromtimestamp(
            int(s3_obj_metadata["createdon"]) / 1000)
    this_dataset_mapping = dataset_mapping[
            "appVersion"][s3_obj_metadata["appVersion"]]["dataset"]
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"].read())) as z:
        contents = z.namelist()
        for json_path in z.namelist():
            dataset_key = os.path.splitext(json_path)[0]
            dataset_version = this_dataset_mapping[dataset_key]
            if dataset_version == "v1":
                dataset_name = dataset_key
            else:
                dataset_name = f"{dataset_key}_{dataset_version}"
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

logger.info(f'Retrieving S3 object for Bucket {workflow_run_properties["source_bucket"]} and Key {workflow_run_properties["source_key"]}')
dataset_mapping = get_dataset_mapping(
        script_location=args["scriptLocation"])
s3_obj = s3_client.get_object(
        Bucket = workflow_run_properties["source_bucket"],
        Key = workflow_run_properties["source_key"])
process_record(
        s3_obj = s3_obj,
        s3_obj_metadata=s3_obj["Metadata"],
        dataset_mapping=dataset_mapping)
