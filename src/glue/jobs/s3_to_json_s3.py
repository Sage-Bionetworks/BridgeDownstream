# Breaks apart the archive files into their own directories
# so that the schema (specific to the taskIdentifier) can be maintained
# by a Glue crawler.
import io
import json
import logging
import os
import sys
import zipfile
import synapseclient
from datetime import datetime
from urllib.parse import urlparse

import boto3
from awsglue.utils import getResolvedOptions


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

glue_client = boto3.client("glue")
s3_client = boto3.client("s3")
ssm_client = boto3.client("ssm")
synapseclient.core.cache.CACHE_ROOT_DIR = '/tmp/.synapseCache'

args = getResolvedOptions(
        sys.argv,
        ["WORKFLOW_NAME",
         "WORKFLOW_RUN_ID",
         "ssm-parameter-name",
         "dataset-mapping"])
workflow_run_properties = glue_client.get_workflow_run_properties(
        Name=args["WORKFLOW_NAME"],
        RunId=args["WORKFLOW_RUN_ID"])["RunProperties"]

def get_dataset_mapping(dataset_mapping_uri):
    dataset_mapping_location = urlparse(dataset_mapping_uri)
    dataset_mapping_bucket = dataset_mapping_location.netloc
    dataset_mapping_key = dataset_mapping_uri.path
    dataset_mapping_fname = os.path.basename(dataset_mapping_key)
    dataset_mapping_file = s3_client.download_file(
            Bucket=dataset_mapping_bucket,
            Key=dataset_mapping_key,
            Filename=dataset_mapping_fname)
    with open(dataset_mapping_fname, "r") as f:
        dataset_mapping = json.load(f)
    logger.debug(f'dataset_mapping: {dataset_mapping}')
    return(dataset_mapping)


def process_record(s3_obj, s3_obj_metadata, dataset_mapping):
    uploaded_on = datetime.strptime(s3_obj_metadata["uploadedon"], '%Y-%m-%dT%H:%M:%S.%fZ')
    this_dataset_mapping = dataset_mapping[
            "appVersion"][s3_obj_metadata["appversion"]]["dataset"]
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"].read())) as z:
        contents = z.namelist()
        logger.debug(f'contents: {contents}')
        for json_path in z.namelist():
            dataset_key = os.path.splitext(json_path)[0]
            dataset_version = this_dataset_mapping[dataset_key]
            dataset_name = dataset_key.lower()
            os.makedirs(dataset_name, exist_ok=True)
            with z.open(json_path, "r") as p:
                j = json.load(p)
                # We inject all S3 metadata into the metadata file
                if dataset_name == "metadata":
                    j["year"] = int(uploaded_on.year)
                    j["month"] = int(uploaded_on.month)
                    j["day"] = int(uploaded_on.day)
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
                            item["year"] = int(uploaded_on.year)
                            item["month"] = int(uploaded_on.month)
                            item["day"] = int(uploaded_on.day)
                            item["recordId"] = s3_obj_metadata["recordid"]
                    else:
                        j["taskIdentifier"] = s3_obj_metadata["taskidentifier"]
                        j["year"] = int(uploaded_on.year)
                        j["month"] = int(uploaded_on.month)
                        j["day"] = int(uploaded_on.day)
                        j["recordId"] = s3_obj_metadata["recordid"]
                output_fname = s3_obj_metadata["recordid"] + ".ndjson"
                output_path = os.path.join(dataset_name, output_fname)
                logger.debug(f'output_path: {output_path}')
                with open(output_path, "w") as f_out:
                    json.dump(j, f_out, indent=None)
                    s3_output_key = os.path.join(
                        workflow_run_properties["app_name"],
                        workflow_run_properties["study_name"],
                        workflow_run_properties["json_prefix"],
                        f"dataset={dataset_name}_{dataset_version}",
                        f"taskIdentifier={s3_obj_metadata['taskidentifier']}",
                        f"year={str(uploaded_on.year)}",
                        f"month={str(uploaded_on.month)}",
                        f"day={str(uploaded_on.day)}",
                        f"recordId={s3_obj_metadata['recordid']}",
                        output_fname)
                with open(output_path, "rb") as f_in:
                    response = s3_client.put_object(
                            Body = f_in,
                            Bucket = workflow_run_properties["json_bucket"],
                            Key = s3_output_key,
                            Metadata = s3_obj_metadata)

logger.info(f"Retrieving dataset mapping at {args['dataset-mapping']}")
dataset_mapping = get_dataset_mapping(
        dataset_mapping_uri=args["dataset-mapping"])
logger.info(f"Logging into Synapse using auth token at {args['ssm-parameter-name']}")
synapse_auth_token = ssm_client.get_parameter(
          Name=args["ssm-parameter-name"],
          WithDecryption=True)
syn = synapseclient.Synapse()
syn.login(authToken=synapse_auth_token, silent=True)
logger.info("Getting messages")
messages = json.loads(workflow_run_properties["messages"])
sts_tokens = {}
for message in messages:
    synapse_data_folder = message["raw_folder_id"]
    if synapse_data_folder not in sts_tokens:
        logger.debug(f"Did not find a cached STS token "
                     f"for {synapse_data_folder}. Getting and adding.")
        sts_token = syn.get_sts_storage_token(
                entity=synapse_data_folder,
                permission="read_only",
                output_format="boto")
        sts_tokens[synapse_data_folder] = sts_token
    logger.info(f"Retrieving S3 object for Bucket {message['source_bucket']} "
                f"and Key {message['source_key']}'")
    bridge_s3_client = boto3.client("s3", **sts_tokens[synapse_data_folder])
    s3_obj = bridge_s3_client.get_object(
            Bucket = message["source_bucket"],
            Key = message["source_key"])
process_record(
        s3_obj = s3_obj,
        s3_obj_metadata=s3_obj["Metadata"],
        dataset_mapping=dataset_mapping)
