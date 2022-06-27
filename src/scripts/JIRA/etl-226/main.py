"""
This script runs the data of each record in the TEST_DATASET through the
validation script at VALIDATION_FUNCTION_PATH and writes the results to
./validation_results.json. Depending on the data and the validation function,
not all data will necessarily be validated against a JSON Schema.

This script must be run from a directory where VALIDATION_FUNCTION_PATH
makes sense.
"""
import sys
import json
import requests
import synapseclient
import boto3

RAW_FOLDER_ID = "syn26253352"
VALIDATION_FUNCTION_PATH = "src/lambda/sns_to_glue"
TEST_DATASET = "syn29276125"
SSM_PARAMETER_NAME = "synapse-bridgedownstream-auth"

sys.path.insert(1, VALIDATION_FUNCTION_PATH)
import app

def get_syn(ssm_parameter_name=SSM_PARAMETER_NAME):
    """Get Synapse client and authenticate with credentials stored in SSM"""
    ssm_client = boto3.client("ssm")
    synapse_auth_token = ssm_client.get_parameter(
              Name=ssm_parameter_name,
              WithDecryption=True)
    syn = synapseclient.Synapse()
    syn.login(authToken=synapse_auth_token["Parameter"]["Value"], silent=True)
    return syn

def get_archive_map(version):
    "Get a specific version or branch of archive-map.json"
    archive_map_url = f"https://raw.githubusercontent.com/Sage-Bionetworks/mobile-client-json/{version}/archive-map.json"
    r = requests.get(archive_map_url)
    return r.json()

def main():
    syn = get_syn()
    dataset = syn.tableQuery(f"select * from {TEST_DATASET}").asDataFrame()
    sts_tokens = dict()
    archive_map = get_archive_map(version="main")
    validation_results = []
    for synapse_id in dataset.id:
        synapse_file = syn.get(synapse_id, downloadFile=False)
        message_parameters = {
                "source_bucket": synapse_file["_file_handle"]["bucketName"],
                "source_key": synapse_file["_file_handle"]["key"],
                "raw_folder_id": RAW_FOLDER_ID
        }
        if message_parameters["raw_folder_id"] not in sts_tokens:
            sts_token = syn.get_sts_storage_token(
                    entity=message_parameters["raw_folder_id"],
                    permission="read_only",
                    output_format="boto")
            sts_tokens[message_parameters["raw_folder_id"]] = sts_token
        validation_result = app.validate_data(
                syn=syn,
                message_parameters=message_parameters,
                archive_map=archive_map,
                sts_tokens=sts_tokens
        )
        validation_results.append(validation_result)
    with open("validation_results.json", "w") as f:
        f.write(json.dumps(validation_results, indent=4))

if __name__ == "__main__":
    main()
