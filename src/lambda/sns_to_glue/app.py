import io
import json
import logging
import os
import requests
import zipfile
import boto3
import jsonschema

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_archive_map(version):
    archive_map_url = f"https://raw.githubusercontent.com/Sage-Bionetworks/mobile-client-json/{version}/archive-map.json"
    r = requests.get(archive_map_url)
    return r.json()


def validate_data(syn, message_parameters, archive_map, aws_session, sts_tokens):
    """
    Check that each piece of JSON data in this record conforms
    to the JSON Schema it claims to conform to. If a JSON does not
    pass validation, then we cannot be certain we have the data
    consumption resources to process this data, and it will be
    flagged as invalid. A record is considered invalid if:

        1. There is no mapping in archive-map.json for at least
        one JSON file in the record.
        2. There is at least one JSON file in the record which does
        not conform to the JSON Schema specified in archive-map.json.

    Otherwise, this record is valid.
    """
    logger.info(f"Retrieving S3 object for Bucket {message_parameters['source_bucket']} "
                f"and Key {message_parameters['source_key']}'")
    bridge_s3_client = boto3.client(
            "s3", **sts_tokens[message_parameters["raw_folder_id"]])
    s3_obj = bridge_s3_client.get_object(
            Bucket = message_parameters["source_bucket"],
            Key = message_parameters["source_key"])
    assessment_id = s3_obj["Metadata"]["assessmentid"]
    assessment_revision = s3_obj["Metadata"]["assessmentrevision"]
    app_id = json.loads(s3_obj["Metadata"]["clientinfo"])["appName"]
    validation_result = {
            "assessmendId": assessment_id,
            "assessmentRevision": assessment_revision,
            "appId": app_id,
            "recordId": s3_obj["Metadata"]["recordid"],
            "errors": {}
    }
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"].read())) as z:
        contents = z.namelist()
        logger.debug(f"zipped contents: {contents}")
        for json_path in contents:
            file_name = os.path.basename(json_path)
            json_schema_url = get_json_schema_url(
                    archive_map=archive_map,
                    file_name=file_name,
                    app_id=app_id,
                    assessment_id=assessment_id,
                    assessment_revision=assessment_revision)
            if json_schema_url is None:
                validation_result["errors"][file_name] = (
                        "Did not find JSON Schema in archive-map.json "
                        f"({os.environ.get('archive_map_version')})."
                )
                continue
            r = requests.get(json_schema_url)
            json_schema = r.json()
            base_uri = os.path.dirname(json_schema_url)
            with z.open(json_path, "r") as p:
                j = json.load(p)
                all_errors = validate_against_schema(
                        data=j,
                        schema=json_schema,
                        base_uri=base_uri
                )
                if len(all_errors) > 0:
                    validation_result["errors"][file_name] = all_errors
    return validation_result


def get_json_schema_url(archive_map, file_name, app_id, assessment_id, assessment_revision):
    json_schema_url = None
    for assessment in archive_map["assessments"]:
        if (assessment["assessmentIdentifier"] == assessment_id
                and assessment["assessmentRevision"] == assessment_revision):
            for file in assessment["files"]:
                if file["filename"] == file_name:
                    return json_schema_url
    for app in archive_map["apps"]:
        if app["appId"] == app_id:
            is_valid_assessment = any([
                    a["assessmentIdentifier"] == assessment_id
                    and a["assessmentRevision"] == assessment_revision
                    for a in app["assessments"]])
            if is_valid_assessment:
                for default_file in app["default"]["files"]:
                    if default_file["filename"] == file_name:
                        json_schema_url = default_file["jsonSchema"]
                        break
                for file in app["anyOf"]:
                    if file["filename"] == file_name:
                        json_schema_url = file["jsonSchema"]
                        break
    if json_schema_url is not None:
        return json_schema_url
    for file in archive_map["anyOf"]:
        if file["filename"] == file_name and file["deprecated"] == False:
            json_schema_url = file["jsonSchema"]
            break
    return json_schema_url


def validate_against_schema(data, schema, base_uri):
    ref_resolver = jsonschema.RefResolver(base_uri=base_uri, referrer=None)
    validator_cls = jsonschema.validators.validator_for(schema)
    validator = validator_cls(schema=schema, resolver=ref_resolver)
    all_errors = [e.message for e in validator.iter_errors(data)]
    return all_errors


def update_sts_tokens(syn, synapse_data_folder, sts_tokens):
    if synapse_data_folder not in sts_tokens:
        logger.debug(f"Did not find a cached STS token "
                     f"for {synapse_data_folder}. Getting and adding.")
        sts_token = syn.get_sts_storage_token(
                entity=synapse_data_folder,
                permission="read_only",
                output_format="boto")
        sts_tokens[synapse_data_folder] = sts_token
    return sts_tokens


def mark_as_invalid(validation_result, sqs_queue):
    pass

def lambda_handler(event, context):
    namespace = os.environ.get('NAMESPACE')
    primary_aws_session = boto3.Session()
    glue_client = primary_aws_session.client("glue")
    messages = {} # indexed by app and study
    sts_tokens = {}
    archive_map = get_archive_map(version=os.environ.get("archive_map_version"))
    for record in event["Records"]:
        body = json.loads(record["body"])
        message = json.loads(body["Message"])
        message_parameters = {
            "source_bucket": message["record"]["s3Bucket"],
            "source_key": message["record"]["s3Key"],
            "raw_folder_id": message["record"]["rawFolderId"]
        }
        sts_tokens = update_sts_tokens(
                syn=syn,
                synapse_data_folder=message_parameters["raw_folder_id"],
                sts_tokens=sts_tokens)
        validation_result = validate_data(
                syn=syn,
                message_parameters=message_parameters,
                archive_map=archive_map,
                aws_session=aws_session,
                sts_tokens=sts_tokens)
        if len(validation_result["errors"]) > 0:
            mark_as_invalid(
                    validation_result = validation_result,
                    sqs_queue=os.environ.get("invalid_sqs"))
        related_studies = message["studyRecords"].keys()
        app = message["appId"]
        for study in related_studies:
            if app in messages:
                pass
            else:
                messages[app] = {study: []}
            if study in messages[app]:
                messages[app][study].append(message_parameters)
            else:
                messages[app][study] = [message_parameters]
    for app in messages:
        for study in messages[app]:
            workflow_name = f"{namespace}-{app}-{study}-S3ToJsonWorkflow"
            logger.info(f'Starting workflow run for workflow {workflow_name}')
            workflow_run = glue_client.start_workflow_run(
                Name=workflow_name)
            glue_client.put_workflow_run_properties(
                Name=workflow_name,
                RunId=workflow_run["RunId"],
                RunProperties={
                    "messages": json.dumps(messages[app][study])
                })
