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
import copy
import io
import json
import logging
import os
import sys
import zipfile
from datetime import datetime
from urllib.parse import urlparse
import boto3
import jsonschema
import requests
import synapseclient
from awsglue.utils import getResolvedOptions


def get_self_ref_schema_list(zipfile_obj: zipfile.ZipFile) -> dict:
    """
    Get the schema url and file name for each JSON file from a Synapse zipfile
    if the file's schema is self referencing

    Args:
        zipfile_obj (zipfile.ZipFile): zipfile object of JSON files
    Returns:
        self_ref_schemas (dict): dictionary of files' filenames and their json schemas
        if their schema is self referencing
    """
    self_ref_schemas = {}
    if "metadata.json" in zipfile_obj.namelist():
        with zipfile_obj.open("metadata.json", "r") as metadata_f:
            metadata = json.load(metadata_f)
            if "files" in metadata:
                for file_info in metadata["files"]:
                    if "jsonSchema" in file_info:
                        self_ref_schemas[file_info["filename"]] = file_info["jsonSchema"]
    return self_ref_schemas


def get_data_mapping(data_mapping_uri):
    """
    Get a mapping to dataset identifiers from S3.

    Args:
        data_mapping_uri (str): The S3 URI in the format s3://bucket-name/key-name

    Returns:
        dict: A mapping to dataset identifiers.
    """
    s3_client = boto3.client("s3")
    logger = logging.getLogger(__name__)
    data_mapping_location = urlparse(data_mapping_uri)
    data_mapping_bucket = data_mapping_location.netloc
    data_mapping_key = data_mapping_location.path[1:]
    data_mapping_fname = os.path.basename(data_mapping_key)
    download_file_args = {
        "Bucket": data_mapping_bucket,
        "Key": data_mapping_key,
        "Filename": data_mapping_fname,
    }
    logger.debug(
        "Calling s3_client.download_file with args: "
        f"{json.dumps(download_file_args)}"
    )
    s3_client.download_file(**download_file_args)
    with open(data_mapping_fname, "r") as f:
        data_mapping = json.load(f)
    logger.debug(f"data_mapping: {data_mapping}")
    return data_mapping


def update_sts_tokens(syn, synapse_data_folder, sts_tokens):
    """
    Update a dict of STS tokens if that token does not yet exist.

    Args:
        syn (synapseclient.Synapse)
        synapse_data_folder (str): Synapse ID of a folder containing Bridge data.
        sts_tokens (dict): A mapping from Synapse IDs to their respective STS
            tokens (also a dict) containing AWS credentials that can be used
            with `boto3.client`.

    Returns:
        sts_tokens (dict)
    """
    logger = logging.getLogger(__name__)
    if synapse_data_folder not in sts_tokens:
        logger.debug(
            f"Did not find a cached STS token "
            f"for {synapse_data_folder}. Getting and adding."
        )
        sts_token = syn.get_sts_storage_token(
            entity=synapse_data_folder, permission="read_only", output_format="boto"
        )
        sts_tokens[synapse_data_folder] = sts_token
    return sts_tokens


def get_archive_map(archive_map_version):
    """
    Get archive-map.json from Sage-Bionetworks/mobile-client-json.

    Args:
        archive_map_version (str): Which tagged version of archive-map.json to get.

    Returns:
        dict: The specified version of archive-map.json
    """
    archive_map_uri_template = (
        "https://raw.githubusercontent.com/Sage-Bionetworks"
        "/mobile-client-json/{version}/archive-map.json"
    )
    archive_map_uri = archive_map_uri_template.format(version=archive_map_version)
    archive_map = requests.get(archive_map_uri)
    archive_map_json = archive_map.json()
    return archive_map_json


def update_json_schemas(s3_obj, archive_map, json_schemas):
    """
    Get JSON Schemas for all files in a zipped S3 object (if the schema exists).
    If a file does not have a JSON Schema, it will not be included in the
    return object.

    Args:
        s3_obj (dict): An S3 object as returned by boto3.get_object.
        archive_map (dict): The dict representation of archive-map.json.
        json_schemas (dict): Maps schema URLs to a JSON Schema.

    Returns:
        json_schemas (list): A list of JSON Schema (dict).
    """
    assessment_id = s3_obj["Metadata"]["assessmentid"]
    assessment_revision = s3_obj["Metadata"]["assessmentrevision"]
    # Currently app_id is fixed "mobile-toolbox". See BRIDGE-3325 / ETL-231.
    app_id = "mobile-toolbox"
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
        contents = z.namelist()
        self_ref_schema_list = get_self_ref_schema_list(z)
        for json_path in contents:
            file_name = os.path.basename(json_path)
            if file_name == "microphone.json":
                file_name = "microphone_levels.json"
            file_metadata = {
                "file_name": file_name,
                "app_id": app_id,
                "record_id": s3_obj["Metadata"]["recordid"],
                "assessment_id": assessment_id,
                "assessment_revision": assessment_revision,
            }
            json_schema = get_json_schema(
                archive_map=archive_map,
                file_metadata=file_metadata,
                json_schemas=json_schemas,
                self_ref_schema_list=self_ref_schema_list,
            )
            if json_schema["schema"] is not None:
                novel_schema = True
                for cached_schema in json_schemas:
                    if cached_schema["schema"]["$id"] == json_schema["schema"]["$id"]:
                        novel_schema = False
                        break
                if novel_schema:
                    json_schemas.append(json_schema)
    return json_schemas


def get_json_schema(archive_map, file_metadata, json_schemas, self_ref_schema_list):
    """
    Fetch the JSON Schema for a given JSON file by either cross-referencing the
    file metadata with archive-map.json from the
    Sage-Bionetworks/mobile-client-json repository or pulling the schema
    directly from the records metadata (self-reference format). The JSON Schema
    specified for a file in metadata.json takes precedence over one specified
    in the archive map.

    The archive map has schemas scoped at three different levels: assessment,
    app, and inter-app (that is, shared among apps). Each scope will be checked
    for this JSON file's schema.

    Args:
        archive_map (dict): The dict representation of archive-map.json.
        file_metadata (dict): A dict with keys
            * assessment_id (str)
            * assessment_revision (str),
            * file_name (str)
            * record_id (str)
            * app_id (str)
        json_schemas (list): A list of cached results from this function
        self_ref_schema_list (dict): A dictionary of files with self referencing schemas

    Returns:
        json_schema (dict): A dictionary with keys
            * url (str)
            * schema (dict)
            * app_id (str)
            * assessment_id (str)
            * assessment_revision (str)
            * file_name (str)
            * archive_map_version (str)
    """
    json_schema = {
        "url": None,
        "schema": None,
        "app_id": file_metadata["app_id"],
        "assessment_id": file_metadata["assessment_id"],
        "assessment_revision": file_metadata["assessment_revision"],
        "file_name": file_metadata["file_name"],
        "archive_map_version": os.environ.get("archive_map_version"),
    }
    # check if file has self-referencing schema
    if file_metadata["file_name"] in self_ref_schema_list:
        json_schema["url"] = self_ref_schema_list[file_metadata["file_name"]]
        json_schema["schema"] = _get_cached_json_schema(
            url=json_schema["url"], json_schemas=json_schemas
        )
        json_schema["archive_map_version"] = None
    else:
        valid_assessments = []
        # Check assessment-specific schemas
        for assessment in archive_map["assessments"]:
            # Collect any assessments which satisfy the min assessment revision
            if (
                assessment["assessmentIdentifier"] == file_metadata["assessment_id"]
                and assessment["assessmentRevision"]
                <= int(file_metadata["assessment_revision"])
            ):
                valid_assessments.append(assessment)
        if valid_assessments:
            # Then find the assessment which is applicable to this data (if any exist)
            revision_distance = [
                int(file_metadata["assessment_revision"]) - a["assessmentRevision"]
                for a in valid_assessments
            ]
            this_assessment = valid_assessments[
                revision_distance.index(min(revision_distance))
            ]
            for file in this_assessment["files"]:
                if file["filename"] == file_metadata["file_name"]:
                    json_schema["url"] = file["jsonSchema"]
                    json_schema["schema"] = _get_cached_json_schema(
                        url=json_schema["url"], json_schemas=json_schemas
                    )
                    return json_schema
        # Check app-specific schemas
        for app in archive_map["apps"]:
            if app["appId"] == file_metadata["app_id"]:
                for default_org in app["default"]:
                    for default_file in default_org["files"]:
                        if default_file["filename"] == file_metadata["file_name"]:
                            json_schema["url"] = default_file["jsonSchema"]
                            break
                for file in app["anyOf"]:
                    if file["filename"] == file_metadata["file_name"]:
                        json_schema["url"] = file["jsonSchema"]
                        break
        if json_schema["url"] is not None:
            json_schema["schema"] = _get_cached_json_schema(
                url=json_schema["url"], json_schemas=json_schemas
            )
            return json_schema
        # Check inter-app schemas
        for file in archive_map["anyOf"]:
            if file["filename"] == file_metadata["file_name"] and "jsonSchema" in file:
                json_schema["url"] = file["jsonSchema"]
                break
        if json_schema["url"] is not None:
            json_schema["schema"] = _get_cached_json_schema(
                url=json_schema["url"], json_schemas=json_schemas
            )
    return json_schema


def _get_cached_json_schema(url, json_schemas):
    """
    Retreive a JSON Schema from a URL

    Args:
        url (str): The URL of the JSON Schema
        json_schemas (list): A list of JSON Schemas (dict)

    Returns:
        (dict) A JSON Schema
    """
    for json_schema in json_schemas:
        if url == json_schema["url"]:
            return json_schema["schema"]
    return requests.get(url).json()


def validate_data(s3_obj, archive_map, json_schemas, dataset_mapping):
    """
    Check that each piece of JSON data in this record conforms
    to the JSON Schema it claims to conform to. If a JSON does not
    pass validation, then we cannot be certain we have the data
    consumption resources to process this data, and it will be
    flagged as invalid. A record is considered invalid if there is
    at least one JSON file in the record which does not conform to
    its JSON Schema.

    Otherwise, this record is valid. If a record comes from an assessment ID/revision
    contained in `dataset_mapping` we do not expect any of the data to conform to
    a JSON Schema, though this record is considered valid (since it has been
    manually mapped to a dataset in `dataset_mapping`).

    Args:
        s3_obj (dict): An S3 object as returned by boto3.get_object.
        archive_map (dict): The dict representation of archive-map.json.
        json_schemas (list): A list of JSON Schema (dict).
        dataset_mapping (dict): The legacy dataset mapping.

    Returns:
        validation_result (dict): A dictionary containing keys
            * assessmentId (str)
            * assessmentRevision (str)
            * appId (str)
            * recordId (str)
            * schema_url (str)
            * errors (dict): mapping file names (str) to their
                validation errors (list[str]).
    """
    logger = logging.getLogger(__name__)
    assessment_id = s3_obj["Metadata"]["assessmentid"]
    assessment_revision = int(s3_obj["Metadata"]["assessmentrevision"])
    # Currently app_id is fixed "mobile-toolbox". See BRIDGE-3325 / ETL-231.
    app_id = "mobile-toolbox"
    validation_result = {
        "assessmentId": assessment_id,
        "assessmentRevision": assessment_revision,
        "appId": app_id,
        "recordId": s3_obj["Metadata"]["recordid"],
        "schema_url": "",
        "errors": {},
    }
    if (
        assessment_id in dataset_mapping["assessmentIdentifier"]
        and str(assessment_revision)
        in dataset_mapping["assessmentIdentifier"][assessment_id]["assessmentRevision"]
    ):
        return validation_result
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
        contents = z.namelist()
        self_ref_schema_list = get_self_ref_schema_list(z)
        for json_path in contents:
            file_name = os.path.basename(json_path)
            if file_name == "microphone.json":
                file_name = "microphone_levels.json"
            json_schema = get_json_schema(
                archive_map=archive_map,
                file_metadata={
                    "file_name": file_name,
                    "app_id": app_id,
                    "record_id": s3_obj["Metadata"]["recordid"],
                    "assessment_id": assessment_id,
                    "assessment_revision": assessment_revision,
                },
                json_schemas=json_schemas,
                self_ref_schema_list=self_ref_schema_list,
            )
            if json_schema["schema"] is None:
                logger.warning(
                    f"Did not find qualifying JSON Schema for {json_path}. "
                    f"in record_id = {validation_result['recordId']}. "
                    f"Unable to validate: {json.dumps(json_schema)}"
                )
                continue
            with z.open(json_path, "r") as p:
                logger.debug(
                        "Validating %s from %s against %s",
                        os.path.basename(json_path),
                        validation_result["recordId"],
                        json_schema["url"]
                )
                j = json.load(p)
                all_errors = validate_against_schema(
                    data=j, schema=json_schema["schema"]
                )
                if len(all_errors) > 0:
                    validation_result["errors"][json_path] = all_errors
    return validation_result


def validate_against_schema(data, schema):
    """
    Validate JSON data against a schema from a given base URI.

    Args:
        data (dict): JSON data
        schema (dict): a JSON Schema

    Returns:
        all_errors (list): A list of validation errors
    """
    validator_cls = jsonschema.validators.validator_for(schema)
    # This is a workaround for this bug
    # https://github.com/python-jsonschema/jsonschema/issues/1012
    if "$id" in schema and schema["$id"].startswith("schemas/v0/"):
        substitute_schema = copy.deepcopy(schema)
        substitute_schema["$id"] = ""
        validator = validator_cls(schema=substitute_schema)
    else:
        validator = validator_cls(schema=schema)
    all_errors = [e.message for e in validator.iter_errors(data)]
    return all_errors


def remove_expected_validation_errors(validation_result, client_info):
    """
    In the first year of MTB there were a number of issues with Android
    data not conforming to the JSON Schema. These aren't very severe
    inconsistincies (mostly missing or superfluous properties), but they
    prevent almost all Android data from being processed. This function
    checks for and drops validation errors which fall into this non-severe
    category. See ETL-312 and ETL-358 for more details.

    Args:
        validation_result (dict): A dictionary containing keys
            * assessmentId (str)
            * assessmentRevision (str)
            * appId (str)
            * recordId (str)
            * errors (dict): mapping file names (str) to their
                validation errors (list[str]).
        client_info (str): A JSON blob containing the client info from the
            relevant S3 object's metadata.

    Returns:
        dict: mapping file names (str) to their (unexpected) validation
              errors (list[str]).
    """
    if not validation_result["errors"]:
        return validation_result["errors"]
    if validation_result["appId"] != "mobile-toolbox":
        return validation_result["errors"]
    if "Android" not in client_info:
        return validation_result["errors"]
    if "metadata.json" in validation_result["errors"]:
        metadata_errors = validation_result["errors"]["metadata.json"]
        allowed_metadata_errors = [
            "'appName' is a required property",
            "'files' is a required property",
        ]
        unexpected_metadata_errors = [
            e for e in metadata_errors if e not in allowed_metadata_errors
        ]
        validation_result["errors"]["metadata.json"] = unexpected_metadata_errors
    if "taskData.json" in validation_result["errors"]:
        taskdata_errors = validation_result["errors"]["taskData.json"]
        unexpected_taskdata_errors = [
            e
            for e in taskdata_errors
            if e != "Additional properties are not allowed ('type' was unexpected)"
        ]
        validation_result["errors"]["taskData.json"] = unexpected_taskdata_errors
    if "weather.json" in validation_result["errors"]:
        weather_errors = validation_result["errors"]["weather.json"]
        unexpected_weather_errors = [
            e for e in weather_errors if e != "'type' is a required property"
        ]
        validation_result["errors"]["weather.json"] = unexpected_weather_errors
    if "motion.json" in validation_result["errors"]:
        motion_errors = validation_result["errors"]["motion.json"]
        allowed_motion_errors = [
            (
                "'acceleration' is not one of ['accelerometer', 'gyro', "
                "'magnetometer', 'attitude', 'gravity', 'magneticField', "
                "'rotationRate', 'userAcceleration']"
            ),
            "'stepPath' is a required property",
        ]
        unexpected_motion_errors = [
            e for e in motion_errors if e not in allowed_motion_errors
        ]
        validation_result["errors"]["motion.json"] = unexpected_motion_errors
    for file_name in list(validation_result["errors"].keys()):
        if not validation_result["errors"][file_name]:
            validation_result["errors"].pop(file_name)
    return validation_result["errors"]


def get_dataset_identifier(json_schema, schema_mapping, dataset_mapping, file_metadata):
    """
    Get a dataset identifier for a file.

    Dataset identifiers are either derived from the JSON Schema $id or
    mapped to within the legacy dataset mapping (if there is no JSON Schema).

    Args:
        json_schema (dict): A JSON Schema.
        schema_mapping (dict): The schema mapping.
        dataset_mapping (dict): The legacy dataset mapping.
        file_metadata (dict): A dict with keys
            * assessment_id (str)
            * assessment_revision (str),
            * file_name (str)
            * record_id (str)
            * app_id (str)

    Returns:
        str: The dataset identifier if it exists, otherwise returns None.
    """
    logger = logging.getLogger(__name__)
    if (
        json_schema is not None
        and "$id" in json_schema
        and json_schema["$id"] in schema_mapping
    ):
        dataset_identifier = schema_mapping[json_schema["$id"]]
        return dataset_identifier
    file_metadata_str = ", ".join(
        [f"{key} = {value}" for key, value in file_metadata.items()]
    )
    if file_metadata["assessment_id"] not in dataset_mapping["assessmentIdentifier"]:
        logger.warning(
            f"Skipping {file_metadata_str} because "
            f"assessmentIdentifier = {file_metadata['assessment_id']} "
            "was not found in dataset mapping."
        )
        return None
    revision_mapping = dataset_mapping["assessmentIdentifier"][
        file_metadata["assessment_id"]
    ]
    if (
        file_metadata["assessment_revision"]
        not in revision_mapping["assessmentRevision"]
    ):
        logger.warning(
            f"Skipping {file_metadata_str} because "
            f"assessmentRevision = {file_metadata['assessment_revision']} "
            "was not found in dataset mapping."
        )
        return None
    dataset_identifier_mapping = revision_mapping["assessmentRevision"][
        file_metadata["assessment_revision"]
    ]
    if file_metadata["file_name"] not in dataset_identifier_mapping:
        logger.warning(
            f"Skipping {file_metadata_str} "
            f"because {file_metadata['file_name']} was not found in the "
            "dataset mapping."
        )
        return None
    dataset_identifier = dataset_identifier_mapping[file_metadata["file_name"]]
    return dataset_identifier


def write_file_to_json_dataset(
    z, json_path, dataset_identifier, s3_obj_metadata, workflow_run_properties
):
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
        workflow_run_properties (dict): The workflow arguments

    Returns:
        output_path (str) The local path the file was written to.
    """
    s3_client = boto3.client("s3")
    logger = logging.getLogger(__name__)
    schema_identifier = dataset_identifier.split("_")[0]
    uploaded_on = datetime.strptime(
        s3_obj_metadata["uploadedon"], "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    os.makedirs(dataset_identifier, exist_ok=True)
    output_path = None
    with z.open(json_path, "r") as p:
        j = json.load(p)
        # We inject all S3 metadata into the metadata file
        if schema_identifier == "ArchiveMetadata":
            j["year"] = int(uploaded_on.year)
            j["month"] = int(uploaded_on.month)
            j["day"] = int(uploaded_on.day)
            for key in s3_obj_metadata:
                j[key] = s3_obj_metadata[key]
        else:  # but only the partition fields and record ID into other files
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
        if output_fname.startswith(("_", ".")):
            # Glue will ignore files which begin with _ or .
            output_fname = f"0{output_fname}"
        output_path = os.path.join(dataset_identifier, output_fname)
        logger.debug(f"output_path: {output_path}")
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
                output_fname,
            )
        with open(output_path, "rb") as f_in:
            response = s3_client.put_object(
                Body=f_in,
                Bucket=workflow_run_properties["json_bucket"],
                Key=s3_output_key,
                Metadata=s3_obj_metadata,
            )
            logger.debug(f"put object response: {json.dumps(response)}")
    return output_path


def process_record(
    s3_obj,
    json_schemas,
    dataset_mapping,
    schema_mapping,
    archive_map,
    workflow_run_properties,
):
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
        json_schemas (list): A list of JSON Schema (dict).
        dataset_mapping (dict): A mapping from assessment ID/revision/filename to
            dataset identifiers
        schema_mapping (dict): A mapping from JSON schema $id to dataset identifiers.
        archive_map (dict): The dict representation of archive-map.json from
            Sage-Bionetworks/mobile-client-json
        workflow_run_properties (dict): The workflow arguments

    Returns:
        None
    """
    logger = logging.getLogger(__name__)
    s3_obj_metadata = s3_obj["Metadata"]
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
        self_ref_schema_list = get_self_ref_schema_list(z)
        contents = z.namelist()
        logger.debug(f"contents: {contents}")
        for json_path in z.namelist():
            file_name = os.path.basename(json_path)
            if file_name == "microphone.json":
                file_name = "microphone_levels.json"
            # Currently app_id is fixed "mobile-toolbox". See BRIDGE-3325 / ETL-231.
            file_metadata = {
                "assessment_id": s3_obj_metadata["assessmentid"],
                "assessment_revision": s3_obj_metadata["assessmentrevision"],
                "file_name": file_name,
                "record_id": s3_obj_metadata["recordid"],
                "app_id": "mobile-toolbox",
            }
            json_schema = get_json_schema(
                archive_map=archive_map,
                file_metadata=file_metadata,
                json_schemas=json_schemas,
                self_ref_schema_list=self_ref_schema_list,
            )
            if json_schema["schema"] is None:
                logger.info(
                    "Did not find a JSON schema in archive-map.json for "
                    f"assessmentId = {s3_obj_metadata['assessmentid']}, "
                    f"assessmentRevision = {s3_obj_metadata['assessmentrevision']}, "
                    f"file_name = {json_path}"
                )
            dataset_identifier = get_dataset_identifier(
                json_schema=json_schema["schema"],
                schema_mapping=schema_mapping,
                dataset_mapping=dataset_mapping,
                file_metadata=file_metadata,
            )
            if dataset_identifier is None:
                continue
            logger.info(f"Writing {json_path} to dataset {dataset_identifier}")
            write_file_to_json_dataset(
                z=z,
                json_path=json_path,
                dataset_identifier=dataset_identifier,
                s3_obj_metadata=s3_obj_metadata,
                workflow_run_properties=workflow_run_properties,
            )


def main():
    # Configure logger
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Instantiate boto clients
    glue_client = boto3.client("glue")
    ssm_client = boto3.client("ssm")
    sqs_client = boto3.client("sqs")
    synapseclient.core.cache.CACHE_ROOT_DIR = "/tmp/.synapseCache"

    # Get job and workflow arguments
    args = getResolvedOptions(
        sys.argv,
        [
            "WORKFLOW_NAME",
            "WORKFLOW_RUN_ID",
            "ssm-parameter-name",
            "dataset-mapping",
            "schema-mapping",
            "archive-map-version",
            "invalid-sqs",
        ],
    )
    workflow_run_properties = glue_client.get_workflow_run_properties(
        Name=args["WORKFLOW_NAME"], RunId=args["WORKFLOW_RUN_ID"]
    )["RunProperties"]
    logger.debug(f"getResolvedOptions: {json.dumps(args)}")
    logger.debug(f"get_workflow_run_properties: {json.dumps(workflow_run_properties)}")

    # Get reference files
    logger.info("Downloading reference files")
    dataset_mapping = get_data_mapping(data_mapping_uri=args["dataset_mapping"])
    schema_mapping = get_data_mapping(data_mapping_uri=args["schema_mapping"])
    archive_map = get_archive_map(archive_map_version=args["archive_map_version"])

    # Authenticate with Synapse
    logger.info(
        f"Logging into Synapse using auth token at {args['ssm_parameter_name']}"
    )
    synapse_auth_token = ssm_client.get_parameter(
        Name=args["ssm_parameter_name"], WithDecryption=True
    )
    syn = synapseclient.Synapse()
    syn.login(authToken=synapse_auth_token["Parameter"]["Value"], silent=True)

    # Load messages to be processed
    logger.info("Loading messages")
    messages = json.loads(workflow_run_properties["messages"])
    sts_tokens = {}
    json_schemas = []

    for message in messages:
        synapse_data_folder = message["raw_folder_id"]
        sts_tokens = update_sts_tokens(
            syn=syn, synapse_data_folder=synapse_data_folder, sts_tokens=sts_tokens
        )
        logger.info(
            f"Retrieving S3 object for Bucket {message['source_bucket']} "
            f"and Key {message['source_key']}'"
        )
        bridge_s3_client = boto3.client("s3", **sts_tokens[synapse_data_folder])
        s3_obj = bridge_s3_client.get_object(
            Bucket=message["source_bucket"], Key=message["source_key"]
        )
        s3_obj["Body"] = s3_obj["Body"].read()
        json_schemas = update_json_schemas(
            s3_obj=s3_obj, archive_map=archive_map, json_schemas=json_schemas
        )
        validation_result = validate_data(
            s3_obj=s3_obj,
            archive_map=archive_map,
            json_schemas=json_schemas,
            dataset_mapping=dataset_mapping,
        )
        validation_result["errors"] = remove_expected_validation_errors(
            validation_result=validation_result,
            client_info=s3_obj["Metadata"]["clientinfo"],
        )
        if validation_result["errors"]:
            for file_name in validation_result["errors"]:
                # limit 10 errors reported per file to avoid redundandant errors
                validation_result["errors"][file_name] = validation_result["errors"][
                    file_name
                ][:10]
            message["validation_result"] = validation_result
            logger.warning(f"Failed validation: {json.dumps(message)}")
            sqs_client.send_message(
                QueueUrl=args["invalid_sqs"], MessageBody=json.dumps(message)
            )
        else:
            process_record(
                s3_obj=s3_obj,
                json_schemas=json_schemas,
                dataset_mapping=dataset_mapping,
                schema_mapping=schema_mapping,
                archive_map=archive_map,
                workflow_run_properties=workflow_run_properties,
            )


if __name__ == "__main__":
    main()
