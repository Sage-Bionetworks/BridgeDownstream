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

def test_get_json_schema(archive_map):
    file_metadata_no_schema = {
            "assessment_id": "spelling",
            "assessment_revision": 5,
            "file_name": "jellybeanz.json",
            "app_id": "mobile-toolbox"
    }
    json_schema_no_schema = get_json_schema(
            archive_map=archive_map,
            file_metadata=file_metadata_no_schema,
            json_schemas={}
    )
    assert isinstance(json_schema_no_schema, dict)
    assert json_schema_no_schema["schema"] is None
    file_metadata_metadata = {
            "assessment_id": "spelling",
            "assessment_revision": 5,
            "file_name": "metadata.json",
            "app_id": "mobile-toolbox"
    }
    json_schema_metadata = get_json_schema(
            archive_map=archive_map,
            file_metadata=file_metadata_metadata,
            json_schemas={}
    )
    assert isinstance(json_schema_metadata, dict)
    assert isinstance(json_schema_metadata["schema"], dict)
    file_metadata_taskdata = {
            "assessment_id": "spelling",
            "assessment_revision": 5,
            "file_name": "taskData.json",
            "app_id": "mobile-toolbox"
    }
    json_schema_taskdata = get_json_schema(
            archive_map=archive_map,
            file_metadata=file_metadata_taskdata,
            json_schemas={}
    )
    assert isinstance(json_schema_taskdata, dict)
    assert isinstance(json_schema_taskdata["schema"], dict)
    file_metadata_motion = {
            "assessment_id": "spelling",
            "assessment_revision": 5,
            "file_name": "motion.json",
            "app_id": "mobile-toolbox"
    }
    json_schema_motion = get_json_schema(
            archive_map=archive_map,
            file_metadata=file_metadata_motion,
            json_schemas={}
    )
    assert isinstance(json_schema_motion, dict)
    assert isinstance(json_schema_motion["schema"], dict)

def test_update_json_schemas(s3_obj, archive_map):
    json_schemas = update_json_schemas(
            s3_obj=s3_obj,
            archive_map=archive_map,
            json_schemas=[]
    )
    assert isinstance(json_schemas, list)
    file_names = [
            "taskData", "taskData.json", "info.json", "motion.json",
            "weather.json", "taskResult.json", "metadata.json"]
    actual_file_names = [j["file_name"] for j in json_schemas]
    for file_name in file_names:
        print(file_name)
        assert file_name in actual_file_names
