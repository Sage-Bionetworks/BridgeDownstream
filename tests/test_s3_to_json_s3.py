import os
import boto3
from src.glue.jobs.s3_to_json_s3 import *
from src.glue.jobs.s3_to_json_s3 import _get_cached_json_schema

def test_get_dataset_mapping(dataset_mapping_uri):
    data_mapping = get_data_mapping(data_mapping_uri=dataset_mapping_uri)
    assert isinstance(data_mapping, dict)

def test_get_schema_mapping(schema_mapping_uri):
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
        assert file_name in actual_file_names

def test_get_dataset_identifier(schema_mapping_uri, dataset_mapping_uri):
    schema_mapping = get_data_mapping(data_mapping_uri=schema_mapping_uri)
    data_mapping = get_data_mapping(data_mapping_uri=dataset_mapping_uri)
    json_schema = {
            "$id": ("https://sage-bionetworks.github.io/mobile-client-json/"
                    "schemas/v2/ArchiveMetadata.json")
    }
    schema_dataset_identifier = get_dataset_identifier(
            json_schema=json_schema,
            schema_mapping=schema_mapping,
            dataset_mapping=data_mapping,
            file_metadata={}
    )
    assert schema_dataset_identifier == "ArchiveMetadata_v1"
    file_metadata = {
            "assessment_id": "dccs",
            "assessment_revision": "5",
            "file_name": "motion.json",
            "app_id": "mobile-toolbox"
    }
    data_dataset_identifier = get_dataset_identifier(
            json_schema={ "schema": None },
            schema_mapping=schema_mapping,
            dataset_mapping=data_mapping,
            file_metadata=file_metadata
    )
    assert data_dataset_identifier == "MotionRecord_v1"

def test_validate_against_schema(s3_obj, metadata_json_schema):
    file_metadata = {
            "assessment_id": s3_obj["Metadata"]["assessmentid"],
            "assessment_revision": s3_obj["Metadata"]["assessmentrevision"],
            "file_name": "metadata.json",
            "app_id": "mobile-toolbox"
    }
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
        with z.open(file_metadata["file_name"], "r") as p:
            j = json.load(p)
            all_errors = validate_against_schema(
                    data=j,
                    schema=metadata_json_schema,
                    base_uri="https://sage-bionetworks.github.io/mobile-client-json/schemas/v2"
            )
            assert isinstance(all_errors, list)
            incorrect_metadata_json_schema = metadata_json_schema
            incorrect_metadata_json_schema["properties"]["cookies"] = { "type": "string" }
            incorrect_metadata_json_schema["required"] = \
                    incorrect_metadata_json_schema["required"] + ["cookies"]
            all_errors = validate_against_schema(
                    data=j,
                    schema=incorrect_metadata_json_schema,
                    base_uri="https://sage-bionetworks.github.io/mobile-client-json/schemas/v2"
            )
            assert len(all_errors) == 1

def test_validate_data(s3_obj, archive_map, dataset_mapping_uri):
    dataset_mapping = get_data_mapping(data_mapping_uri=dataset_mapping_uri)
    validation_result = validate_data(
            s3_obj=s3_obj,
            archive_map=archive_map,
            json_schemas=[],
            dataset_mapping=dataset_mapping)
    assert isinstance(validation_result, dict)
    required_keys = [
            "assessmentId", "assessmentRevision", "appId", "recordId", "errors"]
    for key in required_keys:
        assert key in validation_result
    assert isinstance(validation_result["errors"], dict)

class MockAWSClient:

    def put_object(*args, **kwargs):
        return None

def test_write_metadata_file_to_json_dataset(s3_obj, namespace, monkeypatch):
    monkeypatch.setattr("boto3.client", lambda x : MockAWSClient())
    workflow_run_properties = {
            "namespace": namespace,
            "app_name": "mobile-toolbox",
            "study_name": "dummy-study",
            "json_prefix": "raw-json",
            "json_bucket": "json-bucket"
    }
    partition_fields = ["assessmentid", "year", "month", "day", "recordid"]
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
        output_file = write_file_to_json_dataset(
                z = z,
                json_path="metadata.json",
                dataset_identifier="ArchiveMetadata_v1",
                s3_obj_metadata=s3_obj["Metadata"],
                workflow_run_properties=workflow_run_properties)
        with open(output_file, "r") as f_out:
            metadata = json.load(f_out)
            for metadata_key in s3_obj["Metadata"]:
                assert metadata_key in metadata
            for partition_key in partition_fields:
                assert partition_key in metadata
    with zipfile.ZipFile(io.BytesIO(s3_obj["Body"])) as z:
        output_file = write_file_to_json_dataset(
                z = z,
                json_path="weather.json",
                dataset_identifier="WeatherResult_v1",
                s3_obj_metadata=s3_obj["Metadata"],
                workflow_run_properties=workflow_run_properties)
        with open(output_file, "r") as f_out:
            weather = json.load(f_out)
            for partition_key in partition_fields:
                assert partition_key in weather
