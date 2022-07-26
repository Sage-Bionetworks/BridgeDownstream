"""
Takes a .zip archive provided by Bridge and writes the contents to various
JSON datasets by referencing the assessment ID/revision and filename.
Partition fields are inserted into every JSON, regardless of its schema
identifier. Every field from the S3 object metadata is inserted into
the JSON with schema identifier 'ArchiveMetadata'.

Every JSON dataset has a dataset identifier. The dataset identifier is
derived from the $id property of the associated JSON Schema (if the file
has a JSON schema) or mapped to directly from the assessment ID/revision
and filename for certain older assessments.
"""
import io
import json
import logging
import os
import sys
import zipfile
from datetime import datetime
from urllib.parse import urlparse
import boto3
import requests
import synapseclient
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
         "dataset-mapping",
         "schema-mapping",
         "archive-map-version"])
workflow_run_properties = glue_client.get_workflow_run_properties(
        Name=args["WORKFLOW_NAME"],
        RunId=args["WORKFLOW_RUN_ID"])["RunProperties"]

def get_data_mapping(data_mapping_uri):
    """
    Get a mapping to dataset identifiers from S3.

    Args:
        data_mapping_uri (str): The S3 URI in the format s3://bucket-name/key-name

    Returns:
        dict: A mapping to dataset identifiers.
    """
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
    """
    Get archive-map.json from Sage-Bionetworks/mobile-client-json.

    Args:
        archive_map_version (str): Which tagged version of archive-map.json to get.

    Returns:
        dict: The specified version of archive-map.json
    """
    archive_map_uri_template = ("https://raw.githubusercontent.com/Sage-Bionetworks"
                                "/mobile-client-json/{version}/archive-map.json")
    archive_map_uri = archive_map_uri_template.format(version=archive_map_version)
    archive_map = requests.get(archive_map_uri)
    archive_map_json = archive_map.json()
    return archive_map_json

def get_schema_from_file_info(file_info, file_metadata):
    """
    Get a JSON Schema from an archive-map.json FileInfo object.

    Args:
        file_info (dict): A FileInfo object from archive-map.json
        file_metadata (dict): A dict with keys assessment_id, assessment_revision,
            file_name, and record_id.

    Returns:
        dict: a JSON Schema.
    """
    file_metadata_str = ", ".join(
            [f"{key} = {value}" for key, value in file_metadata.items()])
    if (
            file_info["filename"] == file_metadata["file_name"]
            and "jsonSchema" in file_info
        ):
        try:
            json_schema = requests.get(file_info["jsonSchema"]).json()
            return json_schema
        except requests.RequestException as err:
            logger.warning(
                    f"Skipping {file_metadata_str} because their was an "
                    f"issue getting URL {file_info['jsonSchema']}: {err}")
    return None

def get_json_schema(archive_map, file_metadata):
    """
    Get a JSON Schema for a JSON file.

    JSON files are mapped to JSON Schemas in archive-map.json. Some older
    assessment revisions do not have a JSON Schema.

    Args:
        archive_map (dict): The dict representation of archive-map.json.
        file_metadata (dict): A dict with keys assessment_id, assessment_revision,
            file_name, and record_id.

    Returns:
        dict: A JSON Schema, if it exists. Otherwise returns None.
    """
    # First check universally used files
    for file in archive_map["anyOf"]:
        json_schema = get_schema_from_file_info(
                file_info=file,
                file_metadata=file_metadata)
        if json_schema is not None:
            return json_schema
    # Next check app-specific files
    for app in archive_map["apps"]:
        if app["appId"] == "mobile-toolbox":
            is_valid_assessment = any([
                    a["assessmentIdentifier"] == file_metadata["assessment_id"]
                    and str(a["assessmentRevision"]) == file_metadata["assessment_revision"]
                    for a in app["assessments"]])
            if is_valid_assessment:
                if "default" in app and "files" in app["default"]:
                    for file in app["default"]["files"]:
                        json_schema = get_schema_from_file_info(
                                file_info=file,
                                file_metadata=file_metadata)
                        if json_schema is not None:
                            return json_schema
                if "anyOf" in app:
                    for file in app["anyOf"]:
                        json_schema = get_schema_from_file_info(
                                file_info=file,
                                file_metadata=file_metadata)
                        if json_schema is not None:
                            return json_schema
    # Finally, check assessment-specific files
    for assessment in archive_map["assessments"]:
        if (
                assessment["assessmentIdentifier"] == file_metadata["assessment_id"]
                and str(assessment["assessmentRevision"]) == file_metadata["assessment_revision"]
           ):
            for file in assessment["files"]:
                json_schema = get_schema_from_file_info(
                        file_info=file,
                        file_metadata=file_metadata)
                if json_schema is not None:
                    return json_schema
    return None

def get_dataset_identifier(json_schema, schema_mapping, dataset_mapping, file_metadata):
    """
    Get a dataset identifier for a file.

    Dataset identifiers are either derived from the JSON Schema $id or
    mapped to within the legacy dataset mapping (if there is no JSON Schema).

    Args:
        json_schema (dict): A JSON Schema.
        schema_mapping (dict): The schema mapping.
        dataset_mapping (dict): The legacy dataset mapping.
        file_metadata (dict): A dict with keys assessment_id, assessment_revision,
            file_name, and record_id.

    Returns:
        str: The dataset identifier if it exists, otherwise returns None.
    """
    if json_schema is not None:
        dataset_identifier = schema_mapping[json_schema["$id"]]
        return dataset_identifier
    file_metadata_str = ", ".join(
            [f"{key} = {value}" for key, value in file_metadata.items()])
    if file_metadata["assessment_id"] not in dataset_mapping["assessmentIdentifier"]:
        logger.warning(f"Skipping {file_metadata_str} because "
                       f"assessmentIdentifier = {file_metadata['assessment_id']} "
                       "was not found in dataset mapping.")
        return None
    revision_mapping = \
            dataset_mapping["assessmentIdentifier"][file_metadata["assessment_id"]]
    if file_metadata["assessment_revision"] not in revision_mapping["assessmentRevision"]:
        logger.warning(f"Skipping {file_metadata_str} because "
                       f"assessmentRevision = {file_metadata['assessment_revision']} "
                       "was not found in dataset mapping.")
        return None
    dataset_identifier_mapping = \
            revision_mapping["assessmentRevision"][file_metadata["assessment_revision"]]
    if file_metadata["file_name"] not in dataset_identifier_mapping:
        logger.warning(
                f"Skipping {file_metadata_str} "
                f"because {file_metadata['file_name']} was not found in the "
                "dataset mapping.")
        return None
    dataset_identifier = dataset_identifier_mapping[file_metadata["file_name"]]
    return dataset_identifier

def write_file_to_json_dataset(z, json_path, dataset_identifier, s3_obj_metadata):
    """
    Write a JSON from a zipfile to a JSON dataset.

    Additional fields are inserted at the top-level (if the JSON is an object) or
    at the top-level of each object (if the JSON is a list of objects) before
    the JSON is written as NDJSON (e.g., since this is a single JSON document,
    on a single line) to a JSON dataset in S3. Partition fields are inserted into
    every JSON, regardless of its schema identifier. Every field from the S3
    object metadata is inserted into the JSON with schema identifier
    'ArchiveMetadata'.

    Partition fields are as follows:
        * assessmentid
        * year (as derived from the `uploadedon` field of the S3 object metadata)
        * month ("")
        * day ("")

    Args:
        z (zipfile.Zipfile): The zip archive as provided by Bridge.
        json_path (str): A path relative to the root of `z` to a JSON file.
        dataset_identifier (str): A BridgeDownstream dataset identifier.
        s3_obj_metadata (dict): A dictionary of S3 object metadata.

    Returns:
        None
    """
    schema_identifier = dataset_identifier.split("_")[0]
    uploaded_on = datetime.strptime(
            s3_obj_metadata["uploadedon"],
            '%Y-%m-%dT%H:%M:%S.%fZ')
    os.makedirs(dataset_identifier, exist_ok=True)
    with z.open(json_path, "r") as p:
        j = json.load(p)
        # We inject all S3 metadata into the metadata file
        if schema_identifier == "ArchiveMetadata":
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

def process_record(s3_obj, dataset_mapping, schema_mapping, archive_map):
    """
    Write the contents of a .zip archive stored on S3 to their respective JSON dataset.

    Every JSON dataset has a dataset identifier. The dataset identifier is
    derived from the $id property of the associated JSON Schema (if the file
    has a JSON schema) or mapped to directly from the assessment ID/revision
    and filename for certain older assessments. If there is no mapping from a
    given assessment ID/revision and filename to a dataset identifier, the file
    is skipped. Partition fields are inserted into every JSON, regardless of its
    schema identifier. Every field from the S3 object metadata is inserted into
    the JSON with schema identifier 'ArchiveMetadata'.

    Args:
        s3_obj (dict): An S3 object as returned by boto3.get_object.
        dataset_mapping (dict): A mapping from assessment ID/revision/filename to
            dataset identifiers
        schema_mapping (dict): A mapping from JSON schema $id to dataset identifiers.
        archive_map (dict): The dict representation of archive-map.json from
            Sage-Bionetworks/mobile-client-json

    Returns:
        None
    """
    s3_obj_metadata = s3_obj["Metadata"]
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"].read())) as z:
        contents = z.namelist()
        logger.debug(f'contents: {contents}')
        for json_path in z.namelist():
            file_name = os.path.basename(json_path)
            file_metadata = {
                    "assessment_id": s3_obj_metadata["assessmentid"],
                    "assessment_revision": s3_obj_metadata["assessmentrevision"],
                    "file_name": os.path.basename(json_path),
                    "record_id":s3_obj_metadata["recordid"]
            }
            json_schema = get_json_schema(
                    archive_map=archive_map,
                    file_metadata=file_metadata)
            if json_schema is None:
                logger.info("Did not find a JSON schema in archive-map.json for "
                            f"assessmentId = {s3_obj_metadata['assessmentid']}, "
                            f"assessmentRevision = {s3_obj_metadata['assessmentrevision']}, "
                            f"file = {json_path}")
            dataset_identifier = get_dataset_identifier(
                    json_schema=json_schema,
                    schema_mapping=schema_mapping,
                    dataset_mapping=dataset_mapping,
                    file_metadata=file_metadata)
            if dataset_identifier is None:
                continue
            logger.info(f"Writing {file_name} to dataset {dataset_identifier}")
            write_file_to_json_dataset(
                    z=z,
                    json_path=json_path,
                    dataset_identifier=dataset_identifier,
                    s3_obj_metadata=s3_obj_metadata)

def main():
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
                dataset_mapping=dataset_mapping,
                schema_mapping=schema_mapping,
                archive_map=archive_map)

if __name__ == "__main__":
    main()
