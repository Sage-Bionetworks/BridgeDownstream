# Breaks apart the archive files into their own directories
# so that the schema (specific to the assessmentid) can be maintained
# by a Glue crawler.
import io
import re
import json
import logging
import os
import requests
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
ARCHIVE_MAP_URI = "https://raw.githubusercontent.com/Sage-Bionetworks/mobile-client-json/{version}/archive-map.json"

args = getResolvedOptions(
        sys.argv,
        ["WORKFLOW_NAME",
         "WORKFLOW_RUN_ID",
         "ssm-parameter-name",
         "dataset-mapping",
         "schema-mapping",
         "archive-map-version"])
workflow_run_properties = glue_client.get_workflow_run_properties(
        Name=args["WORKFLOW_NAME"],
        RunId=args["WORKFLOW_RUN_ID"])["RunProperties"]

def get_data_mapping(data_mapping_uri):
    data_mapping_location = urlparse(data_mapping_uri)
    data_mapping_bucket = data_mapping_location.netloc
    data_mapping_key = data_mapping_location.path[1:]
    data_mapping_fname = os.path.basename(data_mapping_key)
    download_file_args = {
            "Bucket":data_mapping_bucket,
            "Key":data_mapping_key,
            "Filename":data_mapping_fname}
    logger.debug("Calling s3_client.download_file with args: "
                 f"{json.dumps(download_file_args)}")
    s3_client.download_file(**download_file_args)
    with open(data_mapping_fname, "r") as f:
        data_mapping = json.load(f)
    logger.debug(f'data_mapping: {data_mapping}')
    return data_mapping

def get_archive_map(archive_map_version):
    archive_map_uri = ARCHIVE_MAP_URI.format(version=archive_map_version)
    archive_map = requests.get(archive_map_uri)
    archive_map_json = archive_map.json()
    return archive_map_json

def get_json_schema(archive_map, assessment_id, assessment_revision, file_name):
    # First check universally used files
    for file in archive_map["anyOf"]:
        if file["filename"] == file_name:
            if "deprecated" in file \
                    and file["deprecated"] is True \
                    and "jsonSchema" not in file:
                continue
            json_schema = requests.get(file["jsonSchema"])
            return json_schema.json()
    # Next check app-specific files
    for app in archive_map["apps"]:
        if app["appId"] == "mobile-toolbox":
            is_valid_assessment = any([
                    a["assessmentIdentifier"] == assessment_id
                    and a["assessmentRevision"] == assessment_revision
                    for a in app["assessments"]])
            if is_valid_assessment:
                for default_file in app["default"]["files"]:
                    if default_file["filename"] == file_name:
                        json_schema = requests.get(default_file["jsonSchema"])
                        return json_schema.json()
                for file in app["anyOf"]:
                    if file["filename"] == file_name:
                        json_schema = requests.get(file["jsonSchema"])
                        return json_schema.json()
    # Finally, check assessment-specific files
    for assessment in archive_map["assessments"]:
        if (assessment["assessmentIdentifier"] == assessment_id
                and assessment["assessmentRevision"] == assessment_revision):
            for file in assessment["files"]:
                if file["filename"] == file_name:
                    json_schema = requests.get(file["jsonSchema"])
                    return json_schema.json()
    return None

def get_dataset_identifier(assessment_id, assessment_revision, file_name, dataset_mapping, record_id):
    if assessment_id not in dataset_mapping["assessmentIdentifier"]:
        logger.warning(f"Skipping {file_name} in recordId = {record_id} because "
                       f"assessmentIdentifier = {assessment_id} was not found "
                       "in dataset mapping.")
        return None
    if (assessment_revision not in
          dataset_mapping["assessmentIdentifier"][assessment_id]["assessmentRevision"]):
        logger.warning(f"Skipping {file_name} in recordId = {record_id} because "
                       f"assessmentRevision = {assessment_revision} was "
                       "not found in dataset mapping for "
                       f"assessmentIdentifier = {assessment_id}.")
        return None
    dataset_identifier_mapping = dataset_mapping["assessmentIdentifier"][assessment_id][
            "assessmentRevision"][assessment_revision]
    if file_name not in dataset_identifier_mapping:
        logger.warning(
                f"Skipping {file_name} in recordId = {record_id} "
                f"because {file_name} was not found in the dataset mapping "
                f"for assessmentRevision = {assessment_revision} "
                f"and assessmentIdentifier = {assessment_id}.")
        return None
    dataset_identifier = dataset_identifier_mapping[file_name]
    return dataset_identifier

def process_record(s3_obj, s3_obj_metadata, dataset_mapping,
        archive_map, schema_mapping):
    uploaded_on = datetime.strptime(s3_obj_metadata["uploadedon"], '%Y-%m-%dT%H:%M:%S.%fZ')
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"].read())) as z:
        contents = z.namelist()
        logger.debug(f'contents: {contents}')
        for json_path in z.namelist():
            file_name = os.path.basename(json_path)
            json_schema = get_json_schema(
                    archive_map=archive_map,
                    assessment_id=s3_obj_metadata["assessmentid"],
                    assessment_revision=int(s3_obj_metadata["assessmentrevision"]),
                    file_name=file_name)
            if json_schema is None:
                logger.info("Did not find a JSON schema in archive-map.json for "
                            f"assessmentId = {s3_obj_metadata['assessmentid']}, "
                            f"assessmentRevision = {s3_obj_metadata['assessmentrevision']}, "
                            f"file = {json_path}")
                dataset_identifier = get_dataset_identifier(
                        assessment_id=s3_obj_metadata["assessmentid"],
                        assessment_revision=s3_obj_metadata["assessmentrevision"],
                        file_name=file_name,
                        dataset_mapping=dataset_mapping,
                        record_id=s3_obj_metadata["recordid"])
                if dataset_identifier is None:
                    continue
            else:
                dataset_identifier = schema_mapping[json_schema["$id"]]
            logger.info(f"Writing {file_name} to dataset {dataset_identifier}")
            data_type = dataset_identifier.split("_")[0]
            os.makedirs(dataset_identifier, exist_ok=True)
            with z.open(json_path, "r") as p:
                j = json.load(p)
                # We inject all S3 metadata into the metadata file
                if data_type == "ArchiveMetadata":
                    j["year"] = int(uploaded_on.year)
                    j["month"] = int(uploaded_on.month)
                    j["day"] = int(uploaded_on.day)
                    for key in s3_obj_metadata:
                        j[key] = s3_obj_metadata[key]
                else: # but only the partition fields and record ID into other files
                    if isinstance(j, list):
                        for item in j:
                            item["assessmentid"] = s3_obj_metadata["assessmentid"]
                            item["year"] = int(uploaded_on.year)
                            item["month"] = int(uploaded_on.month)
                            item["day"] = int(uploaded_on.day)
                            item["recordid"] = s3_obj_metadata["recordid"]
                    else:
                        j["assessmentid"] = s3_obj_metadata["assessmentid"]
                        j["year"] = int(uploaded_on.year)
                        j["month"] = int(uploaded_on.month)
                        j["day"] = int(uploaded_on.day)
                        j["recordid"] = s3_obj_metadata["recordid"]
                output_fname = s3_obj_metadata["recordid"] + ".ndjson"
                output_path = os.path.join(dataset_identifier, output_fname)
                logger.debug(f'output_path: {output_path}')
                with open(output_path, "w") as f_out:
                    json.dump(j, f_out, indent=None)
                    s3_output_key = os.path.join(
                        workflow_run_properties["namespace"],
                        workflow_run_properties["app_name"],
                        workflow_run_properties["study_name"],
                        workflow_run_properties["json_prefix"],
                        f"dataset={dataset_identifier}",
                        f"assessmentid={s3_obj_metadata['assessmentid']}",
                        f"year={str(uploaded_on.year)}",
                        f"month={str(uploaded_on.month)}",
                        f"day={str(uploaded_on.day)}",
                        output_fname)
                with open(output_path, "rb") as f_in:
                    response = s3_client.put_object(
                            Body = f_in,
                            Bucket = workflow_run_properties["json_bucket"],
                            Key = s3_output_key,
                            Metadata = s3_obj_metadata)
                    logger.debug(f"put object response: {json.dumps(response)}")

logger.debug(f"getResolvedOptions: {json.dumps(args)}")
logger.info(f"Retrieving dataset mapping at {args['dataset_mapping']}")
dataset_mapping = get_data_mapping(
        data_mapping_uri=args["dataset_mapping"])
schema_mapping = get_data_mapping(
        data_mapping_uri=args["schema_mapping"])
archive_map = get_archive_map(archive_map_version=args["archive_map_version"])
logger.info(f"Logging into Synapse using auth token at {args['ssm_parameter_name']}")
synapse_auth_token = ssm_client.get_parameter(
          Name=args["ssm_parameter_name"],
          WithDecryption=True)
syn = synapseclient.Synapse()
syn.login(authToken=synapse_auth_token["Parameter"]["Value"], silent=True)
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
            dataset_mapping=dataset_mapping,
            archive_map=archive_map,
            schema_mapping=schema_mapping)
