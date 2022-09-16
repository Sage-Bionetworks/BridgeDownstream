from src.glue.jobs.s3_to_json_s3 import *
from src.glue.jobs.s3_to_json_s3 import _get_cached_json_schema

def test_get_dataset_mapping(artifact_bucket, namespace):
    dataset_mapping_uri = (
            f"s3://{artifact_bucket}/BridgeDownstream/{namespace}/"
            "glue/resources/dataset_mapping.json"
    )
    data_mapping = get_data_mapping(data_mapping_uri=dataset_mapping_uri)
    assert isinstance(data_mapping, dict)

def test_get_schema_mapping(artifact_bucket, namespace):
    schema_mapping_uri = (
            f"s3://{artifact_bucket}/BridgeDownstream/{namespace}/"
            "glue/resources/schema_mapping.json"
    )
    schema_mapping = get_data_mapping(data_mapping_uri=schema_mapping_uri)
    assert isinstance(schema_mapping, dict)

def test_get_archive(artifact_bucket, namespace):
    archive_map = get_archive_map(archive_map_version="v4.4.1")
    assert isinstance(archive_map, dict)

class MockSynapse:

    def get_sts_storage_token(*args, **kwargs):
        sts_storage_token = {
                "bucket": None, "baseKey": None, "accessKeyId": None,
                "secretAccessKey": None, "sessionToken": None, "expiration": None
        }
        return sts_storage_token

def test_update_sts_tokens():
    synapse_data_folder_1 = "syn11111111"
    sts_tokens = update_sts_tokens(
            syn=MockSynapse(),
            synapse_data_folder=synapse_data_folder_1,
            sts_tokens={})
    assert synapse_data_folder_1 in sts_tokens

def test_get_cached_json_schema():
    json_schema = {"url": "url", "schema": "schema", "app_id": None, "assessment_id": None,
            "assessment_revision": None, "file_name": None, "archive_map_version": None}
    cached_json_schema = _get_cached_json_schema(
        url="url",
        json_schemas=[json_schema]
    )
    assert cached_json_schema == "schema"
