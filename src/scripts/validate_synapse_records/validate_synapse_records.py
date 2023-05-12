"""
This script validates record data against its schema by querying
a Synapse file view for records.

The records must use the self-referencing format in their metadata.json file.
Any files in the record which do not have a `jsonSchema` property specified in
their fileInfo object in metadata.json will not be validated.

The validation results are written to `validation_errors.json`.
"""

import argparse
import json
import zipfile
import boto3
import jsonschema
import requests
import synapseclient

def read_args():
    parser = argparse.ArgumentParser(
            description="Validate records in a Synapse file view against their JSON Schema.")
    parser.add_argument("--file-view",
                        required=True,
                        help=("The Synapse ID of a file view containing "
                              "files to be submitted."))
    parser.add_argument("--query",
                        default="select * from {source_table}",
                        help=("Optional. An f-string formatted query which filters the "
                              "file view. Use {source_table} in the FROM clause. "
                              "If this argument is not specified, all files in "
                              "the --file-view will be submitted to the "
                              "--glue-workflow."))
    parser.add_argument("--ssm-parameter",
                        help=("Optional. The name of the SSM parameter containing "
                              "the Synapse personal access token. "
                              "If not provided, cached credentials are used"))
    parser.add_argument("--profile",
                        help=("Optional. The AWS profile to use. Uses the default "
                              "profile if not specified."))
    args = parser.parse_args()
    return args

def get_synapse_client(ssm_parameter=None, aws_session=None):
    """
    Return an authenticated Synapse client.

    Args:
        ssm_parameter (str): Name of the SSM parameter containing the
            BridgeDownstream Synapse password.
        aws_session (boto3.session.Session)

    Returns:
        synapseclient.Synapse
    """
    if ssm_parameter is not None:
        ssm_client = aws_session.client("ssm")
        token = ssm_client.get_parameter(
            Name=ssm_parameter,
            WithDecryption=True)
        syn = synapseclient.Synapse()
        syn.login(authToken=token["Parameter"]["Value"])
    else: # try cached credentials
        syn = synapseclient.login()
    return syn

def get_schemas(synapse_files: list):
    """
    Get schemas for each JSON file in each Synapse file in a list of Synapse files.

    Args:
        synapse_files (list): a list of synapseclient.entity.File objects.

    Returns:
        all_schemas (dict): A dictionary of schemas
            {
                "synapseId": {
                    "filename": "schema_url",
                    ...
                },
            }
    """
    all_schemas = {}
    for synapse_file in synapse_files:
        all_schemas[synapse_file.id] = {}
        with zipfile.ZipFile(synapse_file.path, "r") as z:
            if "metadata.json" in z.namelist():
                with z.open("metadata.json", "r") as metadata_f:
                    metadata = json.load(metadata_f)
                    for file_info in metadata["files"]:
                        if "jsonSchema" in file_info:
                            all_schemas[synapse_file.id][file_info["filename"]] = \
                                    file_info["jsonSchema"]
    return all_schemas

def validate_data(synapse_files: list, schemas: dict):
    """
    Validate Synapse file data against its respective schema.

    Args:
        synapse_files (list): a list of synapseclient.entity.File objects.
        schemas (dict): a collection of schemas as returned by `get_schemas`.

    Returns:
        validation_results (dict): A dictionary of validation errors
            {
                "synapseId": {
                    "filename": ["validation_error", ...],
                    ...
                },
            }
    """
    validation_results = {}
    for synapse_file in synapse_files:
        validation_results[synapse_file.id] = {}
        files_to_validate = schemas[synapse_file.id]
        with zipfile.ZipFile(synapse_file.path, "r") as z:
            for filename in files_to_validate:
                with z.open(filename, "r") as f:
                    data = json.load(f)
                    schema_url = files_to_validate[filename]
                    schema = requests.get(schema_url).json()
                    validation_errors = validate_against_schema(
                            data=data,
                            schema=schema
                    )
                    validation_results[synapse_file.id][filename] = validation_errors
    return validation_results

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
    validator = validator_cls(schema=schema)
    all_errors = [e.message for e in validator.iter_errors(data)]
    return all_errors

def main():
    args = read_args()
    aws_session = boto3.session.Session(
            profile_name=args.profile,
            region_name="us-east-1")
    syn = get_synapse_client(
            ssm_parameter=args.ssm_parameter,
            aws_session=aws_session)
    synapse_df = (
            syn
            .tableQuery(
                args.query.format(source_table=args.file_view)
            )
            .asDataFrame()
    )
    synapse_files = [syn.get(f) for f in synapse_df.id.values]
    schemas = get_schemas(synapse_files=synapse_files)
    validation_errors = validate_data(
            synapse_files=synapse_files,
            schemas=schemas
    )
    with open("validation_errors.json", "w") as f_out:
        json.dump(validation_errors, f_out, indent=2)

if __name__ == "__main__":
    main()
